package ass1.worker;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class TextAnalyzer {

    public static Path performAnalysis(Path input, String type) throws Exception {

        List<String> sentences = splitToSentences(Files.readString(input));

        Path output = Paths.get("/tmp/output-" + System.nanoTime() + ".txt");

        try (BufferedWriter bw = Files.newBufferedWriter(output)) {

            switch (type) {
                case "POS" -> doPOS(sentences, bw);
                case "CONSTITUENCY" -> doConstituency(sentences, bw);
                case "DEPENDENCY" -> doDependency(sentences, bw);
                default -> throw new RuntimeException("Unknown analysis type: " + type);
            }
        }

        return output;
    }


    private static List<String> splitToSentences(String text) {
        List<String> out = new ArrayList<>();
        String[] raw = text.split("(?<=[.!?])\\s+");
        for (String s : raw) {
            s = s.trim();
            if (!s.isEmpty()) out.add(s);
        }
        return out.subList(0, Math.min(out.size(), 20));
    }


    private static void doPOS(List<String> sentences, BufferedWriter bw) throws IOException {
        LexicalizedParser parser = ParserSingleton.get();
        for (String s : sentences) {
            Tree parse = parser.parse(s);
            List<TaggedWord> tw = parse.taggedYield();

            bw.write("Sentence: " + s);
            bw.newLine();

            for (TaggedWord t : tw) {
                bw.write(t.word() + "\t" + t.tag());
                bw.newLine();
            }
            bw.newLine();
        }
    }


    private static void doConstituency(List<String> sentences, BufferedWriter bw) throws IOException {
        LexicalizedParser parser = ParserSingleton.get();
        for (String s : sentences) {
            bw.write("Sentence: " + s);
            bw.newLine();

            Tree parse = parser.parse(s);
            bw.write(parse.toString());
            bw.newLine();
            bw.newLine();
        }
    }


    private static void doDependency(List<String> sentences, BufferedWriter bw) throws IOException {
        LexicalizedParser parser = ParserSingleton.get();
        GrammaticalStructureFactory gsf =
                new PennTreebankLanguagePack().grammaticalStructureFactory();

        for (String s : sentences) {
            bw.write("Sentence: " + s);
            bw.newLine();

            Tree parse = parser.parse(s);
            GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);

            for (TypedDependency td : gs.typedDependenciesCCprocessed()) {
                bw.write(td.toString());
                bw.newLine();
            }

            bw.newLine();
        }
    }
}
