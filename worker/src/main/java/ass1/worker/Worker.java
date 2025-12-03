package ass1.worker;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import ass1.common.AWS;
import ass1.common.MessageFormatter;
import ass1.common.MessageFormatter.AnalyzeFields;
import ass1.common.MessageType;
import ass1.common.S3Helper;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {

    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

    private static final String BUCKET = AWS.getInstance().bucketName;

    public static void main(String[] args) {

        // Worker identity (for logs)
        String workerId = UUID.randomUUID().toString().substring(0, 6);
        System.out.println("[WORKER " + workerId + "] Booted and ready.");

        String tasksQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_TASKS_QUEUE);
        String resultsQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_RESULTS_QUEUE);

        System.out.println("[WORKER " + workerId + "] Online. Awaiting ANALYZE tasks...");

        while (true) {

            List<Message> messages =
                    SqsHelper.receiveMessages(tasksQueueUrl, 1, 3600);  // 30 minutes visibility

            for (Message msg : messages) {

                String body = msg.body();
                MessageType type = MessageFormatter.getMessageType(body);

                if (type != MessageType.ANALYZE) {
                    System.out.println("[WORKER " + workerId + "] Ignored non-ANALYZE: " + type);
                    SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
                    continue;
                }

                AnalyzeFields task = MessageFormatter.parseAnalyzeTask(body);
                String analysisType = task.analysisType();
                String url = task.url();
                String jobId = task.jobId();

                System.out.printf(
                        "[WORKER %s] START | job=%s | type=%s | url=%s%n",
                        workerId, jobId, analysisType, url
                );

                try {

                    // 1) Download
                    Path inputPath = downloadUrlToTemp(url);

                    // 2) Run analysis
                    Path resultFile = TextAnalyzer.performAnalysis(inputPath, analysisType);

                    // 3) Upload to S3
                    String uniqueKey = "results/" +
                            jobId + "-" +
                            analysisType + "-" +
                            UUID.randomUUID() + ".txt";

                    S3Helper.uploadFile(BUCKET, uniqueKey, resultFile);

                    // 4) Send DONE
                    String resultMessage = MessageFormatter.formatWorkerDone(
                            jobId, analysisType, url, uniqueKey);

                    System.out.printf(
                            "[WORKER %s] DONE | job=%s | type=%s | url=%s | key=%s%n",
                            workerId, jobId, analysisType, url, uniqueKey
                    );

                    SqsHelper.sendMessage(resultsQueueUrl, resultMessage);

                } catch (Exception e) {

                    System.err.printf(
                            "[WORKER %s] ERROR | job=%s | type=%s | url=%s | reason=%s%n",
                            workerId, jobId, analysisType, url, e.getMessage()
                    );

                    String errMsg = MessageFormatter.formatWorkerError(
                            jobId, analysisType, url, e.getMessage());

                    SqsHelper.sendMessage(resultsQueueUrl, errMsg);
                }

                // Always delete message after work
                SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
            }

            try { Thread.sleep(300); }
            catch (InterruptedException ignored) {}
        }
    }


    private static Path downloadUrlToTemp(String url) throws IOException {
        System.out.println("[WORKER] Downloading: " + url);
        byte[] bytes = URI.create(url).toURL().openStream().readAllBytes();

        Path tmp = Paths.get("/tmp/input-" + System.nanoTime() + ".txt");
        Files.write(tmp, bytes);

        return tmp;
    }
}
