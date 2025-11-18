package ass1.localapp;

import ass1.common.MessageFormatter;
import ass1.common.MessageFormatter.SummaryDoneFields;
import ass1.common.MessageFormatter.NewTaskFields;
import ass1.common.MessageType;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.UUID;

public class LocalApp {

    private static final String TASKS_QUEUE_NAME = "tasks-queue";

    public static void main(String[] args) {

        String clientId = UUID.randomUUID().toString();
        String callbackQueueName = "response-" + clientId;
        String callbackQueueUrl = SqsHelper.createQueueIfNotExists(callbackQueueName);
        System.out.println("[INFO] Created callback queue: " + callbackQueueName);

        String tasksQueueUrl = SqsHelper.createQueueIfNotExists(TASKS_QUEUE_NAME);

        String fakeInputS3 = "s3://placeholder/input.txt";
        int fakeN = 5;

        String message = MessageFormatter.formatNewTask(fakeInputS3, fakeN, callbackQueueName);
        System.out.println("[DEBUG] Sending NEW_TASK message: " + message);

        SqsHelper.sendMessage(tasksQueueUrl, message);
        System.out.println("[INFO] NEW_TASK message sent to: " + TASKS_QUEUE_NAME);

        System.out.println("[INFO] Waiting for response on: " + callbackQueueName);
        while (true) {
            List<Message> messages = SqsHelper.receiveMessages(callbackQueueUrl, 5);

            for (Message msg : messages) {
                String body = msg.body();
                MessageType type = MessageFormatter.getMessageType(body);

                if (type == MessageType.SUMMARY_DONE) {
                    SummaryDoneFields summary = MessageFormatter.parseSummaryDone(body);
                    System.out.println("=== SUMMARY RECEIVED ===");
                    System.out.println("Job ID: " + summary.jobId());
                    System.out.println("Summary file location: " + summary.summaryS3Path());

                    SqsHelper.deleteMessage(callbackQueueUrl, msg.receiptHandle());
                    return;
                } else {
                    System.out.println("[WARN] Received unrelated message: " + body);
                }
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) { }
        }
    }
}
