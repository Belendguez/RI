import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RemoveDuplicates {
    //Allows to remove to archives with the same exact content.
    //For this we use the Hash we have previously indexed in the class indexFiles 
    //during the creation of the index
    public static void main (String[] args) throws IOException {
        String indexPath = "index";
        String outPath = "indexNoDup";
        String usage = "Arguments: [-index INDEX_PATH]\n\n";
        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-out":
                    outPath = args[++i];
                    break;
                default:
                    System.out.println("Usage: "+usage);
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Directory outDir = FSDirectory.open(Paths.get(outPath));
        IndexWriterConfig iwc = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(outDir, iwc);
        IndexReader reader = DirectoryReader.open(dir);
        int numDocs = reader.numDocs();
        List<String> contentsList = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = reader.document(i);
            String hash = doc.get("hash");
            if (hash != null) {
                if (!contentsList.contains(hash)) {
                    contentsList.add(hash);
                    writer.addDocument(doc);
                }
            }
        }
        writer.close();
        try (IndexReader readerfin = DirectoryReader.open(FSDirectory.open(Path.of(indexPath)))) {
            System.out.println("Original Index has " + readerfin.numDocs() + " documents.  Path: " + indexPath);
        }
        try (IndexReader readerfin = DirectoryReader.open(FSDirectory.open(Path.of(outPath)))) {
            System.out.println("New Index without duplicates has " + readerfin.numDocs() + " documents.   Path: " + outPath);
        }
    }
}
