package ass1.worker;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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
        String tasksQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_TASKS_QUEUE);
        String resultsQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_RESULTS_QUEUE);

        System.out.println("[WORKER] Started. Waiting for ANALYZE messages...");

        while (true) {
            List<Message> messages = SqsHelper.receiveMessages(tasksQueueUrl, 5);

            for (Message msg : messages) {
                String body = msg.body();
                MessageType type = MessageFormatter.getMessageType(body);

                if (type != MessageType.ANALYZE) {
                    System.out.println("[WORKER] Ignoring message of type: " + type);
                    SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
                    continue;
                }

                AnalyzeFields task = MessageFormatter.parseAnalyzeTask(body);

                String analysisType = task.analysisType();
                String url = task.url();
                String jobId = task.jobId();

                System.out.printf("[WORKER] Processing: %s %s [job: %s]%n",
                        analysisType, url, jobId);

                try {
                    // 1) Download input text
                    Path inputPath = downloadUrlToTemp(url);

                    // 2) Perform analysis
                    Path resultFile = TextAnalyzer.performAnalysis(inputPath, analysisType);

                    // 3) Upload result to S3
                    String key = "results/" + jobId + "-" + analysisType + ".txt";
                    S3Helper.uploadFile(BUCKET, key, resultFile);

                    // 4) Send result message to manager
                    String resultMessage = MessageFormatter.formatWorkerDone(
                            jobId, analysisType, url, key);

                    SqsHelper.sendMessage(resultsQueueUrl, resultMessage);
                    System.out.println("[WORKER] Sent result for job: " + jobId);

                } catch (Exception e) {
                    System.err.println("[WORKER] ERROR while processing job " + jobId);
                    e.printStackTrace();
                }

                SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {}
        }
    }

    private static Path downloadUrlToTemp(String url) throws IOException {
        System.out.println("[WORKER] Downloading URL: " + url);
        byte[] bytes = URI.create(url).toURL().openStream().readAllBytes();
        Path tmp = Paths.get("/tmp/input-" + System.nanoTime() + ".txt");
        Files.write(tmp, bytes);
        return tmp;
    }
}
