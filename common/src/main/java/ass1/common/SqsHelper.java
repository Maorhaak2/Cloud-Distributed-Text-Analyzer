package ass1.common;

import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsHelper {

    private static final Region region = Region.US_EAST_1; 
    private static final SqsClient sqs = AWS.getInstance().sqs();

    public static void sendMessage(String queueUrl, String message) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build();
        sqs.sendMessage(request);
    }

    public static List<Message> receiveMessages(String queueUrl, int maxMessages) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(10) // Long polling
                .visibilityTimeout(30)
                .build();

        return sqs.receiveMessage(request).messages();
    }

    public static List<Message> receiveMessages(String queueUrl, int maxMessages, int visibilitySeconds) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(10)        // Long polling
                .visibilityTimeout(visibilitySeconds)
                .build();

        return sqs.receiveMessage(request).messages();
    }


    public static void deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        sqs.deleteMessage(request);
    }

    public static String createQueueIfNotExists(String queueName) {
        try {
            GetQueueUrlRequest getRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            return sqs.getQueueUrl(getRequest).queueUrl();
        } catch (QueueDoesNotExistException e) {
            CreateQueueRequest createRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            sqs.createQueue(createRequest);
            return sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build()).queueUrl();
        }
    }
}
