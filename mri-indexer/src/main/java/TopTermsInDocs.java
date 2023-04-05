import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.PriorityQueue;

public class TopTermsInDocs {
    public static void main(String[] args) throws IOException {
        String indexPath = "index";
        String docID;
        int rango1= -1;
        int rango2 = -1;
        int top = -1;
        String outfile = "output.txt";
        String usage = "Arguments: [-index INDEX_PATH] [-docID docID1-docID2] [-top N] [-outfile OUTFILE_PATH]\n\n";
        //si no se pasa ningun rango se mirara en todos los docs
        //outfile por defecto: output.txt
        //index por defecto: index
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docID":
                    docID = args[++i];
                    rango1 = Integer.parseInt(docID.split("-")[0]);
                    rango2 = Integer.parseInt(docID.split("-")[1]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outfile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (indexPath==null||top==-1){
            System.out.println(usage);
            System.exit(0);
        }

        IndexReader reader = DirectoryReader.open(FSDirectory.open(java.nio.file.Path.of(indexPath)));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));

        if(rango1==-1&&rango2==-1){
            rango1 = 0;
            rango2 = reader.maxDoc()-1;
        }

        // Buscamos solo en el rago que necesitamos
        for (int i = rango1; i <= rango2; i++) {
            Terms terms = reader.getTermVector(i, "contents");

            if (terms == null) continue;  //Si no existe -> al siguiente docID

            // Cola de prioridad para los Top Terms
            PriorityQueue<TermScore> pq = new PriorityQueue<>(top);

            // Loop through the terms and calculate their tf-idf scores
            TermsEnum termsEnum = terms.iterator();
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
                String termText = term.utf8ToString();
                int tf = (int) termsEnum.totalTermFreq();
                int df = reader.docFreq(new Term("contents", term));
                double idf = (double) reader.maxDoc() / df;
                double idflog10 = Math.log10(idf);
                double tfidflog10 = tf * idflog10;

                // Add the term to the priority queue
                pq.add(new TermScore(termText, tf, df, tfidflog10));
                if (pq.size() > top) pq.poll();
            }

            // Print the top terms for the current document
            System.out.println("docId: " + i);
            writer.write("docId: " + i + "\n");
            while (!pq.isEmpty()) {
                TermScore ts = pq.poll();
                System.out.format("%-10s -> tf: %-5d df: %-5d tf-idflog10: %.5f%n", ts.term, ts.tf, ts.df, ts.tfidflog10);
                writer.write(String.format("%-10s -> tf: %-5d df: %-5d tf-idflog10: %.5f%n", ts.term, ts.tf, ts.df, ts.tfidflog10)+ "\n");
            }
            System.out.println();
            writer.write("\n");
        }

        // Close the writer and reader
        writer.close();
        reader.close();
    }

    private static class TermScore implements Comparable<TermScore> {
        String term;
        int tf;
        int df;
        double tfidflog10;

        public TermScore(String term, int tf, int df, double tfidflog10) {
            this.term = term;
            this.tf = tf;
            this.df = df;
            this.tfidflog10 = tfidflog10;
        }

        @Override
        public int compareTo(TermScore other) {
            return Double.compare(this.tfidflog10, other.tfidflog10);
        }
    }
}