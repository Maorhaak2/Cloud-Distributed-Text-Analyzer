package ass1.manager;

import ass1.common.MessageFormatter;
import ass1.common.MessageFormatter.*;
import ass1.common.MessageType;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.*;

public class Manager {

    private static final String TASKS_QUEUE = "tasks-queue";
    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

    private static final Map<String, JobState> jobs = new HashMap<>();
    private static final int TOTAL_SIMULATED_TASKS = 9;  // במקום קובץ מ-S3
    private static boolean terminateMode = false;

    public static void main(String[] args) {

        String tasksQueueUrl = SqsHelper.createQueueIfNotExists(TASKS_QUEUE);
        String workerTasksUrl = SqsHelper.createQueueIfNotExists(WORKER_TASKS_QUEUE);
        String workerResultsUrl = SqsHelper.createQueueIfNotExists(WORKER_RESULTS_QUEUE);

        System.out.println("[MANAGER] Started.");

        while (true) {

            // --- Step 1: NEW_TASK or TERMINATE ---
            if (!terminateMode) {
                List<Message> taskMessages = SqsHelper.receiveMessages(tasksQueueUrl, 5);
                for (Message msg : taskMessages) {
                    handleTaskMessage(msg, tasksQueueUrl, workerTasksUrl);
                }
            }

            // --- Step 2: WORKER_DONE ---
            List<Message> resultMessages = SqsHelper.receiveMessages(workerResultsUrl, 10);
            for (Message msg : resultMessages) {
                handleWorkerDoneMessage(msg, workerResultsUrl);
            }

            // --- Step 3: Exit if in terminate mode and all jobs are done ---
            if (terminateMode && jobs.isEmpty()) {
                System.out.println("[MANAGER] All jobs completed. Terminating.");
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
        }
    }

    private static void handleTaskMessage(Message msg, String tasksQueueUrl, String workerTasksUrl) {
        String body = msg.body();
        MessageType type = MessageFormatter.getMessageType(body);

        if (type == MessageType.NEW_TASK) {
            NewTaskFields task = MessageFormatter.parseNewTask(body);
            String jobId = UUID.randomUUID().toString();

            System.out.println("[MANAGER] NEW_TASK received. jobId: " + jobId);

            int n = task.n();
            int numWorkersNeeded = (int) Math.ceil(TOTAL_SIMULATED_TASKS / (double) n);
            System.out.println("[MANAGER] Task has " + TOTAL_SIMULATED_TASKS + " jobs. Requesting " + numWorkersNeeded + " workers.");

            // 🔹 Simulate 9 messages (3 types × 3 URLs)
            List<String> urls = List.of(
                    "https://www.gutenberg.org/files/1659/1659-0.txt",
                    "https://www.gutenberg.org/files/1660/1660-0.txt",
                    "https://www.gutenberg.org/files/1661/1661-0.txt"
            );
            List<String> types = List.of("POS", "CONSTITUENCY", "DEPENDENCY");

            for (String url : urls) {
                for (String analysis : types) {
                    String analyzeMsg = MessageFormatter.formatAnalyzeTask(analysis, url, jobId);
                    SqsHelper.sendMessage(workerTasksUrl, analyzeMsg);
                }
            }

            jobs.put(jobId, new JobState(task.callbackQueue(), TOTAL_SIMULATED_TASKS));
            SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
        }

        else if (type == MessageType.TERMINATE) {
            TerminateFields term = MessageFormatter.parseTerminate(body);
            System.out.println("[MANAGER] Received TERMINATE request. No new tasks will be processed.");
            terminateMode = true;
            SqsHelper.deleteMessage(tasksQueueUrl, msg.receiptHandle());
        }
    }

    private static void handleWorkerDoneMessage(Message msg, String resultsQueueUrl) {
        String body = msg.body();
        MessageType type = MessageFormatter.getMessageType(body);

        if (type != MessageType.WORKER_DONE) {
            System.out.println("[MANAGER] Ignoring message of type: " + type);
            return;
        }

        WorkerDoneFields done = MessageFormatter.parseWorkerDone(body);
        String jobId = done.jobId();

        if (!jobs.containsKey(jobId)) {
            System.out.println("[MANAGER] Unknown jobId: " + jobId);
            return;
        }

        JobState state = jobs.get(jobId);
        state.results.add(done.resultInfo());

        System.out.println("[MANAGER] Got result for job " + jobId + ": " + done.resultInfo());
        SqsHelper.deleteMessage(resultsQueueUrl, msg.receiptHandle());

        if (state.results.size() == state.expectedResults) {
            String summary = "Job " + jobId + " completed with " + state.results.size() + " results.";
            String summaryMsg = MessageFormatter.formatSummaryDone(jobId, summary);
            String callbackUrl = SqsHelper.createQueueIfNotExists(state.callbackQueueName);
            SqsHelper.sendMessage(callbackUrl, summaryMsg);
            System.out.println("[MANAGER] Sent SUMMARY_DONE to " + state.callbackQueueName);
            jobs.remove(jobId);
        }
    }

    private static class JobState {
        String callbackQueueName;
        int expectedResults;
        List<String> results = new ArrayList<>();

        JobState(String callbackQueueName, int expectedResults) {
            this.callbackQueueName = callbackQueueName;
            this.expectedResults = expectedResults;
        }
    }
}
