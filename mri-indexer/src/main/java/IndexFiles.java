/**
 *Alumno 1: 
 *Alumno 2 Belen Domínguez Álvarez user:belen.domingueza
*/

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

public class IndexFiles{
    public static int depth = -1;
    public static boolean contentsStored = false;
    public static boolean contentsTermVectors = false;
    public static Properties properties = new Properties();
    public static String onlyLines;
    public static String onlyFilesn;
    public static String notFilesn;

    /** Index all text files under a directory. With their respective properties and other relevant information.*/
    public static void main(String[] args) throws Exception {
        properties.load(new FileReader("mri-indexer/src/main/resources/config.properties"));
        onlyLines = properties.getProperty("onlyLines");
        onlyFilesn = properties.getProperty("onlyFiles");
        notFilesn = properties.getProperty("notFiles");
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update || -create] [-numThreads NUM_THREADS] [-depth DEPTH] [-contentsStored] [-contentsTermVectors]\n\n"
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

        //Check if the path to the directory is valid or not
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

        // Executor for the ThreadPool
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        //Each Folder goes to his respective thread.
        ArrayList<Path> partialIndexPaths = new ArrayList<>();
        for (Path folder : getFolders(docsPath)) {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents
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

        /* Wait up to 1 hour to finish all the previously submitted jobs.*/
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }


        //Creating the general index for the subindex generated
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


        //Reading the index created.
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
        String fileName = file.getFileName().toString();
        String extension = "";
        int lastIndex = fileName.lastIndexOf('.');
        if (lastIndex != -1) {
            extension = fileName.substring(lastIndex);
        }
        if(!extension.equals("")){
            //If possible we avoid folders.
            //If the two rules are active, identify which is the first one that appears in 
            //config properties and apply it. 
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
            //We look one by one
            } else if (notFilesn != null) {
                String[] notFiles = notFilesn.split(" ");
                return !Arrays.asList(notFiles).contains(extension);
            } else if (onlyFilesn != null) {
                String[] onlyFiles = onlyFilesn.split(" ");
                return Arrays.asList(onlyFiles).contains(extension);
            } else {
                // We index all archives.
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
                Document doc = new Document();

                //Path
                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                //Modified
                doc.add(new LongPoint("modified", lastModified));

                //Content
                //We check the content stored
                FieldType fieldType;
                if (contentsStored) {
                    fieldType = new FieldType(TextField.TYPE_STORED);
                } else {
                    fieldType = new FieldType(TextField.TYPE_NOT_STORED);
                }
                //Check TermsVectors
                if (contentsTermVectors) {
                    fieldType.setStoreTermVectors(true);
                }
                //Check OnlyLines
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

                //information about the archive
                BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);

                //Hostname and thread
                doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));

                //Type
                String fileType = "otro";
                if (attrs.isRegularFile()) {
                    fileType = "regular file";
                } else if (attrs.isDirectory()) {
                    fileType = "directory";
                } else if (attrs.isSymbolicLink()) {
                    fileType = "symbolic link";
                }
                doc.add(new StringField("type", fileType, Field.Store.YES));

                //Size
                long sizeKb = attrs.size() / 1024;
                doc.add(new StoredField("sizeKb", sizeKb));

                //Date
                FileTime creationTime = attrs.creationTime();
                FileTime lastAccessTime = attrs.lastAccessTime();
                FileTime lastModifiedTime = attrs.lastModifiedTime();
                doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));

                // Date in Lucene Format
                String creationTimeLucene = DateTools.dateToString(new Date(creationTime.toMillis()), DateTools.Resolution.MILLISECOND);
                String lastAccessTimeLucene = DateTools.dateToString(new Date(lastAccessTime.toMillis()), DateTools.Resolution.MILLISECOND);
                String lastModifiedTimeLucene = DateTools.dateToString(new Date(lastModifiedTime.toMillis()), DateTools.Resolution.MILLISECOND);
                doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));

                //HASH SHA-256 for the RemoveDuplicates
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(Files.readAllBytes(file));
                String hashString = Base64.getEncoder().encodeToString(hash);
                Field hashField = new StringField("hash", hashString, Field.Store.YES);
                doc.add(hashField);


                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    System.out.println("adding " + file);
                    writer.addDocument(doc);
                } else {

                    System.out.println("updating " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }
}

