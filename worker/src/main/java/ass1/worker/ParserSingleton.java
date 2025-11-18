package ass1.worker;

import edu.stanford.nlp.parser.lexparser.LexicalizedParser;

public class ParserSingleton {

    private static final String MODEL =
            "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";

    private static LexicalizedParser instance = null;

    public static synchronized LexicalizedParser get() {
        if (instance == null) {
            System.out.println("[WORKER] Loading Stanford Parser Model...");
            instance = LexicalizedParser.loadModel(MODEL);
            System.out.println("[WORKER] Parser Loaded.");
        }
        return instance;
    }
}