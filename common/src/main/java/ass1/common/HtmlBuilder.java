package ass1.common;

import java.util.Map;

public class HtmlBuilder {

    private static final String BUCKET = "text-jobs-bucket";

    public static String build(Map<String, String> results) {

        StringBuilder sb = new StringBuilder();

        sb.append("<html>\n");
        sb.append("<body>\n");

        results.forEach((key, value) -> {

            String[] parts = value.split("\t");

            String analysis = parts[0];
            String inputUrl = parts[1];
            String outputPart = parts[2];

            String inputLink = "<a href=\"" + inputUrl + "\">" + inputUrl + "</a>";

            String finalOutput;

            if (!outputPart.startsWith("ERROR:")) {
                String presigned = S3Helper.generatePresignedUrl(BUCKET, outputPart);
                finalOutput = "<a href=\"" + presigned + "\">" + outputPart + "</a>";
            } else {
                finalOutput = outputPart; 
            }

            sb.append(analysis)
              .append(": ")
              .append(inputLink)
              .append(" ")
              .append(finalOutput)
              .append("<br/>\n");
        });

        sb.append("</body>\n");
        sb.append("</html>\n");

        return sb.toString();
    }
}
