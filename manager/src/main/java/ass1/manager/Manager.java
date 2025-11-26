package ass1.manager;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import ass1.common.*;
import software.amazon.awssdk.services.sqs.model.Message;

public class Manager {

    // ----------------------------------------------------
    //  CONSTANTS & CONFIG
    // ----------------------------------------------------
    private static final String MANAGER_INPUT_QUEUE = "tasks-queue";
    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

    // SQS URLs (filled in constructor)
    private final String managerInputQueueUrl;
    private final String workerTasksQueueUrl;
    private final String workerResultsQueueUrl;

    // Executor for parallel Job handling
    private final ExecutorService jobExecutor;

    // Tracks all active jobs
    private final ConcurrentHashMap<String, Job> jobs;

    // Control flags
    private volatile boolean acceptingNewTasks = true;

    public Manager() {

        // Create/get queues
        this.managerInputQueueUrl = SqsHelper.createQueueIfNotExists(MANAGER_INPUT_QUEUE);
        this.workerTasksQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_TASKS_QUEUE);
        this.workerResultsQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_RESULTS_QUEUE);

        // Thread pool for parallel jobs
        this.jobExecutor = Executors.newFixedThreadPool(10);

        // Job state map
        this.jobs = new ConcurrentHashMap<>();
    }


    // ----------------------------------------------------
    //   NESTED JOB CLASS (STATE HOLDER ONLY)
    // ----------------------------------------------------
    private static class Job {
        final String jobId;
        final String inputFileS3;
        final int n;
        final String callbackQueue;
        final int totalTasks;

        final ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();
        final AtomicInteger completed = new AtomicInteger(0);
        final CompletableFuture<Void> finished = new CompletableFuture<>();

        Job(String jobId, String inputFileS3, int n, String callbackQueue, int totalTasks){
            this.jobId = jobId;
            this.inputFileS3 = inputFileS3;
            this.n = n;
            this.callbackQueue = callbackQueue;
            this.totalTasks = totalTasks;
        }
    }

    // ----------------------------------------------------
    //  MAIN ENTRY POINT
    // ----------------------------------------------------
    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.start();
    }

    // Start the manager (starts listeners)
    public void start() {
        // TODO – add listeners in next steps
        System.out.println("[Manager] Manager started.");

        startLocalAppListener();
        startWorkerResultsListener();
    }

    // ----------------------------------------------------
    //   PLACEHOLDER FOR LISTENERS
    // ----------------------------------------------------
    private void startLocalAppListener() {
    Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
            System.out.println("[Manager] LocalApp Listener started.");
            
            while (acceptingNewTasks) {
                try {
                    // מקבל עד 10 הודעות בפול ארוך
                    List<Message> messages = SqsHelper.receiveMessages(managerInputQueueUrl, 10);
                    
                    for (Message msg : messages) {
                        String body = msg.body();
                        MessageType type = MessageFormatter.getMessageType(body);
                        
                        switch (type) {
                            
                            case NEW_TASK -> {
                                System.out.println("[Manager] Received NEW_TASK");
                                jobExecutor.submit(() -> handleNewTask(msg));
                            }
                            
                            case TERMINATE -> {
                                System.out.println("[Manager] Received TERMINATE");
                                handleTerminate(msg);   // נעשה בזה שלב נפרד
                            }
                        }
                        
                        // מוחק את ההודעה מהתור
                        SqsHelper.deleteMessage(managerInputQueueUrl, msg.receiptHandle());
                    }
                    
                } catch (Exception e) {
                    System.err.println("[Manager] Error in LocalApp Listener: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            System.out.println("[Manager] LocalApp Listener stopped.");
        }
    });

    t.setDaemon(true);  // what?????????????????????????????????????????????????????
    t.start();
}


    private void startWorkerResultsListener() {
        // Stage 3 will implement
    }

    // ----------------------------------------------------
    //   PLACEHOLDER FOR JOB HANDLING
    // ----------------------------------------------------
    private void handleNewTask(Message msg) {
    try {
        // ---------------------------------------------
        // 1. Parse the NEW_TASK message
        // ---------------------------------------------
        String body = msg.body();
        MessageFormatter.NewTaskFields fields = MessageFormatter.parseNewTask(body);

        String inputS3 = fields.inputFileS3();
        int n = fields.n();
        String callbackQueue = fields.callbackQueue();

        System.out.println("[Manager] Starting NEW_TASK: " + inputS3);

        // ---------------------------------------------
        // 2. Download the input file from S3
        // ---------------------------------------------
        String bucketName = AWS.getInstance().bucketName;  // תוודא שקיים אצלך
        Path tempFile = Path.of("/tmp/" + UUID.randomUUID() + ".txt");

        S3Helper.downloadFile(bucketName, inputS3, tempFile);

        // ---------------------------------------------
        // 3. Parse lines → each represents a task
        // Format: <analysisType> \t <url>
        // ---------------------------------------------
        List<String> lines = java.nio.file.Files.readAllLines(tempFile);

        int totalTasks = lines.size();
        String jobId = UUID.randomUUID().toString();

        // Create Job object
        Job job = new Job(jobId, inputS3, n, callbackQueue, totalTasks);
        jobs.put(jobId, job);

        System.out.println("[Manager] Job " + jobId + " loaded with " + totalTasks + " tasks.");

        // ---------------------------------------------
        // 4. Send each task to Worker queue
        // ---------------------------------------------
        for (String line : lines) {
            String[] p = line.split("\t");
            String analysisType = p[0];
            String url = p[1];

            String workerMsg = MessageFormatter.formatAnalyzeTask(
                    analysisType,
                    url,
                    jobId
            );

            SqsHelper.sendMessage(workerTasksQueueUrl, workerMsg);
        }

        // ---------------------------------------------
        // 5. Compute required workers
        // ---------------------------------------------
        int neededWorkers = (int) Math.ceil((double) totalTasks / n);

        System.out.println("[Manager] Job " + jobId + ": Need " + neededWorkers + " workers.");

        // This method must be implemented in Ec2Helper by you
        Ec2Helper.createWorkersIfNeeded(neededWorkers);

        // ---------------------------------------------
        // 6. Wait for workers to finish
        // ---------------------------------------------
        System.out.println("[Manager] Job " + jobId + " waiting for results...");
        job.finished.join();   // waits efficiently (no CPU usage)

        System.out.println("[Manager] Job " + jobId + " finished all tasks.");

        // ---------------------------------------------
        // 7. Build summary HTML
        // ---------------------------------------------
        String summaryHtml = HtmlBuilder.build(job.results);  // נכתוב בהמשך אם תרצה

        String summaryKey = "summaries/" + jobId + ".html";
        Path summaryFilePath = Path.of("/tmp/" + jobId + ".html");

        java.nio.file.Files.writeString(summaryFilePath, summaryHtml);

        S3Helper.uploadFile(bucketName, summaryKey, summaryFilePath);

        // ---------------------------------------------
        // 8. Send SUMMARY_DONE back to LocalApp
        // ---------------------------------------------
        String doneMsg = MessageFormatter.formatSummaryDone(jobId, summaryKey);
        SqsHelper.sendMessage(job.callbackQueue, doneMsg);

        System.out.println("[Manager] Summary sent to client for job " + jobId);

        // cleanup
        jobs.remove(jobId);

    } catch (Exception e) {
        System.err.println("[Manager] Error in handleNewTask: " + e.getMessage());
        e.printStackTrace();
    }
}


    private void handleTerminate(Message msg) {
        // Stage 5 will implement
    }
}
