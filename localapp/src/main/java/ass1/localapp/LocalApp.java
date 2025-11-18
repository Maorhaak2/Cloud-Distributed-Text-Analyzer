package ass1.localapp;

import ass1.common.AWS;
import ass1.common.Ec2Helper;
import ass1.common.S3Helper;
import ass1.common.SqsHelper;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import software.amazon.awssdk.services.sqs.model.Message;

public class LocalApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java LocalApp <input.txt> <n> <output.txt> [terminate]");
            return;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = args.length >= 4 && (args[3] == "terminate" || args[3] == "TERMINATE");


        // Generate unique response queue name
        String responseQueueName = "response-" + UUID.randomUUID();
        String responseQueueUrl = SqsHelper.createQueueIfNotExists(responseQueueName);
        System.out.println("[INFO] Created callback queue: " + responseQueueName);

        // Start manager EC2 if needed
        if (!Ec2Helper.isManagerRunning()) {
            System.out.println("[INFO] No manager detected. Starting one now...");
            Ec2Helper.startManagerInstance();
        }

        // Upload input file to S3
        String bucketName = AWS.getInstance().bucketName;
        String s3Key = "input/" + UUID.randomUUID() + ".txt";
        S3Helper.uploadFile(bucketName, s3Key, Path.of(inputPath));
        System.out.println("[INFO] Uploaded input file to S3: " + s3Key);

        // Send NEW_TASK message to manager
        String messageBody = terminate
                ? "TERMINATE\t" + responseQueueName
                : "NEW_TASK\t" + "s3://" + bucketName + "/" + s3Key + "\t" + n + "\t" + responseQueueName;
        String tasksQueueUrl = SqsHelper.createQueueIfNotExists("tasks-queue");
        SqsHelper.sendMessage(tasksQueueUrl, messageBody);
        System.out.println("[INFO] Sent message to manager: " + messageBody);

        // Wait for summary response on response queue
        System.out.println("[INFO] Waiting for summary on queue: " + responseQueueName);
        boolean received = false;
        while (!received) {
            List<Message> messages = SqsHelper.receiveMessages(responseQueueUrl, 5);
            for (Message msg : messages) {
                if (msg.body().startsWith("SUMMARY_DONE")) {
                    String[] parts = msg.body().split("\t");
                    String summaryKey = parts[1];
                    S3Helper.downloadFile(bucketName, summaryKey, Path.of(outputPath));
                    System.out.println("[INFO] Downloaded summary to: " + outputPath);
                    SqsHelper.deleteMessage(responseQueueUrl, msg.receiptHandle());
                    received = true;
                    break;
                } else {
                    SqsHelper.deleteMessage(responseQueueUrl, msg.receiptHandle());
                }
            }
            Thread.sleep(2000); // Poll every 2 seconds
        }

        System.out.println("[INFO] LocalApp finished.");
    }
}
