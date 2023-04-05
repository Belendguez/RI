
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

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import javax.swing.plaf.synth.SynthLookAndFeel;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexFiles{
    public static int depth = -1;
    public static boolean contentsStored = false;
    public static boolean contentsTermVectors = false;
    public static Properties properties = new Properties();
    public static String onlyLines;

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
                case "-contentsStored":
                    contentsStored = true;
                    break;
                case "-contentsTermVectors":
                    contentsTermVectors = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        properties.load(new FileReader("mri-indexer/src/main/resources/config.properties"));
        onlyLines = properties.getProperty("onlyLines");

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
            String foldersinpath = folder.toString().replaceFirst(docsPath, "");
            Directory subindexpath = FSDirectory.open(Path.of(indexPath+"/temp/"+foldersinpath));
            partialIndexPaths.add(Path.of(indexPath+"/temp/"+foldersinpath));
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
        writerc.close();
        deleteDirectory(new File(indexPath+"/temp"));

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

    public static boolean extCompatible(Path file) throws IOException {
        String onlyFilesn = properties.getProperty("onlyFiles");
        String notFilesn = properties.getProperty("notFiles");
        String fileName = file.getFileName().toString();
        String extension = "";
        int lastIndex = fileName.lastIndexOf('.');
        if (lastIndex != -1) {
            extension = fileName.substring(lastIndex);
        }
        if(!extension.equals("")){//Evitamos carpetas
            // Si estan las dos identificar cual aparece primero en config.properties y aplicarla
            if (onlyFilesn != null && notFilesn != null) {
                int onlyFilesIndex = -1;
                int notFilesIndex = -1;
                int lineCount = 0;
                try (BufferedReader reader = new BufferedReader(new FileReader("mri-indexer/src/main/resources/config.properties"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("onlyFiles")) {
                            onlyFilesIndex = lineCount;
                        } else if (line.startsWith("notFiles")) {
                            notFilesIndex = lineCount;
                        }
                        lineCount++;
                    }
                }
                if (onlyFilesIndex<notFilesIndex) {
                    String[] onlyFiles = onlyFilesn.split(" ");
                    return Arrays.asList(onlyFiles).contains(extension);
                } else {
                    String[] notFiles = notFilesn.split(" ");
                    return !Arrays.asList(notFiles).contains(extension);
                }
            //Sino mirar una a una
            } else if (notFilesn != null) {
                String[] notFiles = notFilesn.split(" ");
                return !Arrays.asList(notFiles).contains(extension);
            } else if (onlyFilesn != null) {
                String[] onlyFiles = onlyFilesn.split(" ");
                return Arrays.asList(onlyFiles).contains(extension);
            } else {
                // Sino se indexan todos los archivos
                return true;
            }
        }return true;
    }


    public static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
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
           if (depth > 0){
                Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), depth, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                        } catch (@SuppressWarnings("unused") IOException e) {
                            //e.printStackTrace(System.err);
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
        if (extCompatible(file)) {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();

                //INDEXAMOS EL PATH
                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                //INDEXAMOS EL MODIFIED
                doc.add(new LongPoint("modified", lastModified));

                //INDEXAMOS EL CONTENT
                //comprobamos content stored
                FieldType fieldType;
                if (contentsStored) {
                    fieldType = new FieldType(TextField.TYPE_STORED);
                } else {
                    fieldType = new FieldType(TextField.TYPE_NOT_STORED);
                }
                //comprobamos TermsVectors
                if (contentsTermVectors) {
                    fieldType.setStoreTermVectors(true);
                }
                //Comprobamos OnlyLines
                if (onlyLines != null) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                        StringBuilder contents = new StringBuilder();
                        String line;
                        int lineCount = 0;
                        while ((line = reader.readLine()) != null && lineCount < Integer.parseInt(onlyLines)) {
                            contents.append(line).append("\n");
                            lineCount++;
                        }
                        doc.add(new Field("contents", contents.toString(), fieldType));
                    }
                } else {
                    if (contentsStored) {
                        doc.add(new Field("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining("\n")), fieldType));
                    } else {
                        doc.add(new Field("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)), fieldType));
                    }
                }

                //Obtener información del archivo
                BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);

                //INDEXAMOS EL HOSTNAME Y EL THREAD
                doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));

                //INDEXAMOS EL TIPO
                String fileType = "otro";
                if (attrs.isRegularFile()) {
                    fileType = "regular file";
                } else if (attrs.isDirectory()) {
                    fileType = "directory";
                } else if (attrs.isSymbolicLink()) {
                    fileType = "symbolic link";
                }
                doc.add(new StringField("type", fileType, Field.Store.YES));

                //INDEXAMOS EL TAMAÑO
                long sizeKb = attrs.size() / 1024;
                doc.add(new StoredField("sizeKb", sizeKb));

                //INDEXAMOS LAS FECHAS
                FileTime creationTime = attrs.creationTime();
                FileTime lastAccessTime = attrs.lastAccessTime();
                FileTime lastModifiedTime = attrs.lastModifiedTime();
                doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));

                // Indexar las fechas en el formato de Lucene
                String creationTimeLucene = DateTools.dateToString(new Date(creationTime.toMillis()), DateTools.Resolution.MILLISECOND);
                String lastAccessTimeLucene = DateTools.dateToString(new Date(lastAccessTime.toMillis()), DateTools.Resolution.MILLISECOND);
                String lastModifiedTimeLucene = DateTools.dateToString(new Date(lastModifiedTime.toMillis()), DateTools.Resolution.MILLISECOND);
                doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));


                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    System.out.println("adding " + file);
                    writer.addDocument(doc);
                } else {

                    System.out.println("updating " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            }
        }
    }
}

