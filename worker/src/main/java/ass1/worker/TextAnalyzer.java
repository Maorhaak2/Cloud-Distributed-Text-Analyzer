package ass1.worker;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;
import edu.stanford.nlp.trees.TypedDependency;

public class TextAnalyzer {

    public static Path performAnalysis(Path input, String type) throws Exception {

        // Output file under /tmp
        Path output = Paths.get("/tmp/output-" + System.nanoTime() + ".txt");

        // Load parser components once
        LexicalizedParser parser = ParserSingleton.get();
        GrammaticalStructureFactory gsf =
                new PennTreebankLanguagePack().grammaticalStructureFactory();

        long start = System.currentTimeMillis();
        int sentenceCount = 0;

        // DocumentPreprocessor streams the file and performs sentence splitting
        try (FileReader fr = new FileReader(input.toFile());
             BufferedWriter bw = Files.newBufferedWriter(output)) {

            DocumentPreprocessor dp = new DocumentPreprocessor(fr);

            // Iterate over detected sentences
            for (List<HasWord> sentenceTokens : dp) {
                sentenceCount++;

                if (sentenceCount % 50 == 0) {
                    System.out.println("[WORKER] Parsed " + sentenceCount + " sentences");
                }

                try {
                    Tree parse = parser.parse(sentenceTokens);

                    // Write original sentence text
                    bw.write("SENTENCE " + sentenceCount + ": " + tokensToString(sentenceTokens));
                    bw.newLine();

                    // Select analysis type
                    switch (type) {
                        case "POS" -> {
                            for (TaggedWord t : parse.taggedYield()) {
                                bw.write(t.word() + "\t" + t.tag());
                                bw.newLine();
                            }
                        }
                        case "CONSTITUENCY" -> {
                            bw.write(parse.pennString());
                            bw.newLine();
                        }
                        case "DEPENDENCY" -> {
                            GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
                            for (TypedDependency td : gs.typedDependenciesCCprocessed()) {
                                bw.write(td.toString());
                                bw.newLine();
                            }
                        }
                    }

                    bw.newLine();

                } catch (Exception e) {
                    // Skip individual sentence errors
                    bw.write("[ERROR parsing sentence " + sentenceCount + "]");
                    bw.newLine();
                    bw.newLine();
                }
            }
        }

        System.out.println("[WORKER] FINISHED. Total sentences: " + sentenceCount);
        return output;
    }

    // Convert token list to string
    private static String tokensToString(List<HasWord> tokens) {
        StringBuilder sb = new StringBuilder();
        for (HasWord w : tokens) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(w.word());
        }
        return sb.toString();
    }

    // Pretty-print a constituency tree
    private static String prettyPrintTree(Tree tree) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        new TreePrint("penn3").printTree(tree, pw);
        return sw.toString();
    }
}

