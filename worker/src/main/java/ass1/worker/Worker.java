package ass1.worker;

import ass1.common.MessageFormatter;
import ass1.common.MessageFormatter.AnalyzeFields;
import ass1.common.MessageType;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class Worker {

    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

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
                    continue;
                }

                AnalyzeFields task = MessageFormatter.parseAnalyzeTask(body);
                String analysisType = task.analysisType();
                String url = task.url();
                String jobId = task.jobId();

                System.out.printf("[WORKER] Processing: %s %s [job: %s]%n", analysisType, url, jobId);

                String resultInfo = "processed-" + analysisType.toLowerCase() + "-result";

                String resultMessage = MessageFormatter.formatWorkerDone(jobId, analysisType, url, resultInfo);
                SqsHelper.sendMessage(resultsQueueUrl, resultMessage);
                System.out.println("[WORKER] Sent result for job: " + jobId);

                SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
        }
    }
}
