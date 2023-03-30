
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.EnumSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexFiles{
    public static int depth = -1;

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update || -create] [-numThreads NUM_THREADS]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles\n";

        String indexPath = "index";
        String docsPath = null;
        Integer numThreads = null;
        boolean create = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-update":
                    create = false;
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-depth":
                    depth = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (numThreads == null)
            numThreads = Runtime.getRuntime().availableProcessors();

        if(depth == 0){
            System.out.println("No se indexara nada, depth = " + depth);
            System.exit(0);
        }

        //Comprobar directorio
        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println(
                    "Document directory '"
                            + docDir.toAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }
        Date start = new Date();
        System.out.println("Indexing to directory '" + indexPath + "'...");

        // Optional: for better indexing performance, if you
        // are indexing many documents, increase the RAM
        // buffer.  But if you do this, increase the max heap
        // size to the JVM (eg add -Xmx512m or -Xmx1g):
        //
        // iwc.setRAMBufferSizeMB(256.0);

        // Creamos el executor
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        //Cada carpeta se la mandamos a su thread
        ArrayList<Path> partialIndexPaths = new ArrayList<>();
        List<Thread> threads = new ArrayList<Thread>();
        for (Path folder : getFolders(docsPath)) {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }
            String foldersin = folder.toString().replaceFirst(docsPath, "");
            Directory subindexpath = FSDirectory.open(Path.of(indexPath+"\\"+foldersin));
            partialIndexPaths.add(Path.of(indexPath+"\\"+foldersin));
            IndexWriter writer = new IndexWriter(subindexpath, iwc);
            final Runnable worker = new WorkerThread(folder, writer);
            executor.execute(worker);
            }


        executor.shutdown();

        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }


        //Creamos el indice general de los subindices
        System.out.println("Indices parciales creados, creando indice general: "+ indexPath);
        Directory indexpath = FSDirectory.open(Path.of(indexPath));
        IndexWriterConfig cnf = new IndexWriterConfig(new StandardAnalyzer());
        if (create) {
            // Create a new index in the directory, removing any
            // previously indexed documents:
            cnf.setOpenMode(OpenMode.CREATE);
        } else {
            // Add new documents to an existing index:
            cnf.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }
        IndexWriter writerc = new IndexWriter(indexpath, cnf);
        for (Path path : partialIndexPaths){
            writerc.addIndexes(FSDirectory.open(path));
        }
        writerc.commit();
        writerc.close();

        Date end = new Date();


        //Leemos el indice general
        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of(indexPath)))) {
            System.out.println(
                    "Indexed "
                            + reader.numDocs()
                            + " documents in "
                            + (end.getTime() - start.getTime())
                            + " milliseconds");
            if (reader.numDocs() > 100 && System.getProperty("smoketester") == null) {
                throw new RuntimeException(
                        "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
            }
        }
    }


    private static Path[] getFolders(String dir) throws IOException {
        return Files.walk(Paths.get(dir), 1)
                .skip(1)
                .filter(Files::isDirectory)
                .toArray(Path[]::new);
    }



    static class WorkerThread implements Runnable {

        private final Path folder;
        private final IndexWriter writer;

        public WorkerThread(Path folder, IndexWriter writer) {
            this.folder = folder;
            this.writer = writer;
        }

        @Override
        public void run() {
            // NOTE: if you want to maximize search performance,
                // you can optionally call forceMerge here.  This can be
                // a terribly costly operation, so generally it's only
                // worth it when your index is relatively static (ie
                // you're done adding documents to it):
                //
                // writer.forceMerge(1);
            try {
                System.out.println("Soy el hilo "+ Thread.currentThread().getName()+" y voy a indexar las entradas de la carpeta: "+folder);
                indexDocs(writer, folder);
                writer.close();
                System.out.println("Soy el hilo "+ Thread.currentThread().getName()+" y he acabado de indexar las entradas de la carpeta: "+folder);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }




    /**
     * Indexes the given file using the given writer, or if a directory is given, recurses over files
     * and directories found under the given directory.
     *
     * <p>NOTE: This method indexes one document per input file. This is slow. For good throughput,
     * put multiple documents into your input file(s). An example of this is in the benchmark module,
     * which can create "line doc" files, one document per line, using the <a
     * href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be stored
     * @param path The file to index, or the directory to recurse into to find files to index
     * @throws IOException If there is a low-level I/O error
     */
    public static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            if (depth > 0) {
                // Si la profundidad es mayor que 0, solo busca hasta el nivel indicado
                //SALTA EXCEPCION DE QUE NO TIENE PERMISO PERO FUNCIONA-----------------------------------------
                Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), depth, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                        } catch (@SuppressWarnings("unused") IOException e) {
                            e.printStackTrace(System.err);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            else{
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            try {
                                indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                            } catch (@SuppressWarnings("unused") IOException ignore) {
                                ignore.printStackTrace(System.err);
                                // don't index files that can't be read.
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }}else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }



    /** Indexes a single document */
    public static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path".  Use a
            // field that is indexed (i.e. searchable), but don't tokenize
            // the field into separate words and don't index term frequency
            // or positional information:
            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            // Add the last modified date of the file a field named "modified".
            // Use a LongPoint that is indexed (i.e. efficiently filterable with
            // PointRangeQuery).  This indexes to milli-second resolution, which
            // is often too fine.  You could instead create a number based on
            // year/month/day/hour/minutes/seconds, down the resolution you require.
            // For example the long value 2011021714 would mean
            // February 17, 2011, 2-3 PM.
            doc.add(new LongPoint("modified", lastModified));

            // Add the contents of the file to a field named "contents".  Specify a Reader,
            // so that the text of the file is tokenized and indexed, but not stored.
            // Note that FileReader expects the file to be in UTF-8 encoding.
            // If that's not the case searching for special characters will fail.
            doc.add(
                    new TextField(
                            "contents",
                            new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
}

