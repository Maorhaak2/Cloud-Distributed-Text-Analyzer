package ass1.common;

import java.util.Map;

public class HtmlBuilder {

    public static String build(Map<String, String> results) {
        StringBuilder sb = new StringBuilder();

        sb.append("<html>\n");
        sb.append("<body>\n");
        sb.append("<pre>\n");

        results.forEach((url, resultInfo) -> {
            // resultInfo is: "<analysisType> <result>" → but we need full formatting
            int sep = resultInfo.indexOf("\t");
            String analysis = resultInfo.substring(0, sep);
            String output = resultInfo.substring(sep + 1);

            sb.append(analysis)
              .append(": ")
              .append(url)
              .append("  ")
              .append(output)
              .append("\n");
        });

        sb.append("</pre>\n");
        sb.append("</body>\n");
        sb.append("</html>\n");

        return sb.toString();
    }
}
