package ass1.localapp;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import ass1.common.AWS;
import ass1.common.Ec2Helper;
import ass1.common.MessageFormatter;
import ass1.common.S3Helper;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

public class LocalApp {
    private static final String BUCKET_NAME = AWS.getInstance().bucketName;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java LocalApp <input.txt> <n> <output.txt> [terminate]");
            return;
        }

        long start =  System.currentTimeMillis();

        String inputPath = args[0];
        String outputPath = args[1];
        int n = Integer.parseInt(args[2]);
        // LocalApp.java - קוד מתוקן
        boolean terminate = args.length >= 4 && "terminate".equalsIgnoreCase(args[3]);


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
        
        String s3Key = "input/" + UUID.randomUUID() + ".txt";
        S3Helper.uploadFile(BUCKET_NAME, s3Key, Path.of(inputPath));
        System.out.println("[INFO] Uploaded input file to S3: " + s3Key);

        // Send NEW_TASK message to manager
        String messageBody = MessageFormatter.formatNewTask(
                "s3://" + BUCKET_NAME + "/" + s3Key,
                n,
                responseQueueName,
                terminate
        );

        String tasksQueueUrl = SqsHelper.createQueueIfNotExists("tasks-queue");
        SqsHelper.sendMessage(tasksQueueUrl, messageBody);
        System.out.println("[INFO] Sent message to manager: " + messageBody);

        // Wait for summary response on response queue
        System.out.println("[INFO] Waiting for summary on queue: " + responseQueueName);
        boolean received = false;
        while (!received) {
            List<Message> messages = SqsHelper.receiveMessages(responseQueueUrl, 5);
            for (Message msg : messages) {
                System.out.println("[DEBUG] Received message: '" + msg.body() + "'");
                if (msg.body().startsWith("SUMMARY_DONE")) {
                    System.out.println("[DEBUG] Parsing SUMMARY_DONE message: " + msg.body());

                    String[] parts = msg.body().trim().split("\\s+");
                    String summaryKey = parts[2];
                    System.out.println("[DEBUG] Extracted summary key: " + summaryKey);

                    S3Helper.downloadFile(BUCKET_NAME, summaryKey, Path.of(outputPath));
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
        
        long end = System.currentTimeMillis();
        System.out.println("[INFO] Total time: " + (end - start) + " ms");
        System.out.println("[INFO] LocalApp finished.");
    }
}
