package ass1.common;

public class MessageFormatter {

    private static final String TAB = "\t";

    public static MessageType getMessageType(String message) {
        String[] parts = message.trim().split("\\s+");
        System.out.println("[test] " + parts[0]);
        return MessageType.valueOf(parts[0]);
    }


    // ----------------------------------------------------
    //  LOCALAPP  → MANAGER
    // ----------------------------------------------------

    public static String formatNewTask(String inputFileS3Path,
                                       int n,   
                                       String callbackQueueName,
                                       boolean terminate) {

        return MessageType.NEW_TASK + TAB  
                + inputFileS3Path + TAB
                + n + TAB
                + callbackQueueName + TAB
                + (terminate ? "terminate" : "NOT-terminate");
    }


    // parse: NEW_TASK <inputS3> <n> <callbackQueue>
    public static NewTaskFields parseNewTask(String body) {
        String[] p = body.split(TAB);
        return new NewTaskFields(
                p[1],               // inputFileS3Path
                Integer.parseInt(p[2]),
                p[3],               // callbackQueueName
                p[4].equals("terminate")
        );
    }

    public record NewTaskFields(String inputFileS3,
                                int n,
                                String callbackQueue,
                                boolean terminate) {}



    // ----------------------------------------------------
    //  MANAGER → WORKER
    // ----------------------------------------------------

    public static String formatAnalyzeTask(String analysisType,
                                           String url,
                                           String jobId) {

        return MessageType.ANALYZE + TAB
                + analysisType + TAB
                + url + TAB
                + jobId;
    }

    public static AnalyzeFields parseAnalyzeTask(String body) {
        String[] p = body.split(TAB);
        return new AnalyzeFields(
                p[1],        // analysisType (כמו "POS")
                p[2],        // url
                p[3]         // jobId unique
        );
    }

    public record AnalyzeFields(String analysisType,
                                String url,
                                String jobId) { }


    // ----------------------------------------------------
    //  WORKER → MANAGER
    // ----------------------------------------------------

    public static String formatWorkerDone(String jobId,
                                          String analysisType,
                                          String url,
                                          String resultInfo) {

        return MessageType.WORKER_DONE + TAB
                + jobId + TAB
                + analysisType + TAB
                + url + TAB
                + resultInfo;
    }

    public static WorkerDoneFields parseWorkerDone(String body) {
        String[] p = body.split(TAB);
        return new WorkerDoneFields(
                p[1],  // jobId
                p[2],  // analysisType
                p[3],  // url
                p[4]   // resultInfo
        );
    }

    public record WorkerDoneFields(String jobId,
                                   String analysisType,
                                   String url,
                                   String resultInfo) { }


    // ----------------------------------------------------
    //  MANAGER → LOCALAPP
    // ----------------------------------------------------

    public static String formatSummaryDone(String jobId,
                                           String summaryS3Path) {

        return MessageType.SUMMARY_DONE + TAB
                + jobId + TAB
                + summaryS3Path;
    }

    public static SummaryDoneFields parseSummaryDone(String body) {
        String[] p = body.split(TAB);
        return new SummaryDoneFields(
                p[1],   // jobId
                p[2]    // summaryS3Path
        );
    }

    public record SummaryDoneFields(String jobId,
                                    String summaryS3Path) { }

    // ----------------------------------------------------
    //  WORKER → MANAGER   (ERROR)
    // ----------------------------------------------------

    public static String formatWorkerError(String jobId,
                                        String analysisType,
                                        String url,
                                        String errorMsg) {

        return MessageType.WORKER_ERROR + TAB
                + jobId + TAB
                + analysisType + TAB
                + url + TAB
                + sanitize(errorMsg);
    }

    // parse: WORKER_ERROR  <jobId> <analysisType> <url> <errorMsg>
    public static WorkerErrorFields parseWorkerError(String body) {
        String[] p = body.split(TAB, 5);  
        // split into max 5 fields → חשוב כדי שההודעה לא תישבר

        return new WorkerErrorFields(
                p[1],   // jobId
                p[2],   // analysisType
                p[3],   // url
                p[4]    // error message
        );
    }

    // wrapper record
    public record WorkerErrorFields(String jobId,
                                    String analysisType,
                                    String url,
                                    String errorMsg) { }

    // מנקה טאבים ושורות חדשות בהודעת שגיאה
    private static String sanitize(String msg) {
        if (msg == null)
            return "Unknown error";

        return msg
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .trim();
    }
}
