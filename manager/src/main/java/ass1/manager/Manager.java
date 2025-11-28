package ass1.manager;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import ass1.common.AWS;
import ass1.common.Ec2Helper;
import ass1.common.HtmlBuilder;
import ass1.common.MessageFormatter;
import ass1.common.MessageType;
import ass1.common.S3Helper;
import ass1.common.SqsHelper;
import software.amazon.awssdk.services.sqs.model.Message;

public class Manager {

    private static final String MANAGER_INPUT_QUEUE = "tasks-queue";
    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

    private final String managerInputQueueUrl;
    private final String workerTasksQueueUrl;
    private final String workerResultsQueueUrl;

    // ======================================================================
    // SINGLE ThreadPool for all job tasks (NEW_TASK + TERMINATE)
    // ======================================================================
    private final ExecutorService jobPool = Executors.newFixedThreadPool(10);

    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private volatile boolean acceptingNewTasks = true;


    public Manager() {
        this.managerInputQueueUrl = SqsHelper.createQueueIfNotExists(MANAGER_INPUT_QUEUE);
        this.workerTasksQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_TASKS_QUEUE);
        this.workerResultsQueueUrl = SqsHelper.createQueueIfNotExists(WORKER_RESULTS_QUEUE);
    }


    private static class Job {
        final String jobId;
        final String callbackQueue;
        final int totalTasks;
        final ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();
        final AtomicInteger completed = new AtomicInteger(0);
        final CompletableFuture<Void> finished = new CompletableFuture<>();

        Job(String jobId, String callbackQueue, int totalTasks) {
            this.jobId = jobId;
            this.callbackQueue = callbackQueue;
            this.totalTasks = totalTasks;
        }
    }


    public static void main(String[] args) {
        new Manager().start();
    }


    public void start() {
        System.out.println("[Manager] Manager started.");
        startLocalAppListener();
        startWorkerResultsListener();
    }


    // ========================================================================
    // LISTENER: LOCALAPP INPUT
    // ========================================================================
    private void startLocalAppListener() {
        Thread t = new Thread(() -> {
            System.out.println("[Manager] LocalApp Listener started.");

            while (acceptingNewTasks || !jobs.isEmpty()) {

                try {
                    List<Message> messages = SqsHelper.receiveMessages(managerInputQueueUrl, 10);

                    for (Message msg : messages) {

                        MessageType type = MessageFormatter.getMessageType(msg.body());

                        switch (type) {

                            case NEW_TASK -> {
                                if (acceptingNewTasks) {
                                    System.out.println("[Manager] NEW_TASK received");

                                    // submit NEW_TASK to jobPool
                                    jobPool.submit(() -> handleNewTask(msg));

                                } else {
                                    System.out.println("[Manager] Ignoring NEW_TASK (termination mode)");
                                }
                            }

                            case TERMINATE -> {
                                System.out.println("[Manager] TERMINATE received");

                                // submit TERMINATE to jobPool
                                jobPool.submit(() -> handleTerminate(msg));
                            }
                        }

                        SqsHelper.deleteMessage(managerInputQueueUrl, msg.receiptHandle());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            jobPool.shutdown();
            System.out.println("[Manager] LocalApp Listener stopped.");
        });

        t.start();
    }


    // ========================================================================
    // LISTENER: WORKER RESULTS
    // ========================================================================
    private void startWorkerResultsListener() {
        Thread t = new Thread(() -> {
            System.out.println("[Manager] WorkerResults Listener started.");

            while (acceptingNewTasks || !jobs.isEmpty()) {
                try {
                    List<Message> messages = SqsHelper.receiveMessages(workerResultsQueueUrl, 10);

                    for (Message msg : messages) {
                        handleWorkerDone(msg.body());
                        SqsHelper.deleteMessage(workerResultsQueueUrl, msg.receiptHandle());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("[Manager] WorkerResults Listener stopped.");
        });

        t.start();
    }


    // ========================================================================
    // HANDLER: WORKER DONE
    // ========================================================================
    private void handleWorkerDone(String body) {
        MessageFormatter.WorkerDoneFields f = MessageFormatter.parseWorkerDone(body);

        Job job = jobs.get(f.jobId());
        if (job == null)
            return;
        String jobKey = UUID.randomUUID().toString();
        String value = f.analysisType() + "\t" + f.url() + "\t" + f.resultInfo();
        job.results.put(jobKey, value);

        int done = job.completed.incrementAndGet();
        System.out.println("[Manager] Job " + job.jobId + ": " + done + "/" + job.totalTasks);

        if (done == job.totalTasks) {
            job.finished.complete(null);
        }
    }


    // ========================================================================
    // HANDLER: NEW TASK
    // ========================================================================
    private void handleNewTask(Message msg) {

        try {
            MessageFormatter.NewTaskFields f = MessageFormatter.parseNewTask(msg.body());

            String fullS3 = f.inputFileS3();
            String bucket = AWS.getInstance().bucketName;
            String key = fullS3.substring(("s3://" + bucket + "/").length());

            Path temp = Path.of("/tmp/" + UUID.randomUUID() + ".txt");
            S3Helper.downloadFile(bucket, key, temp);

            List<String> lines = java.nio.file.Files.readAllLines(temp);

            String jobId = UUID.randomUUID().toString();
            Job job = new Job(jobId, f.callbackQueue(), lines.size());
            jobs.put(jobId, job);

            System.out.println("[Manager] Job created. " + lines.size() + " tasks.");

            // send worker tasks
            for (String line : lines) {
                String[] p = line.split("\t");
                String msgOut = MessageFormatter.formatAnalyzeTask(p[0], p[1], jobId);
                SqsHelper.sendMessage(workerTasksQueueUrl, msgOut);
            }

            // create workers if needed
            int neededWorkers = (int) Math.ceil(lines.size() / (double) f.n());
            Ec2Helper.createWorkersIfNeeded(neededWorkers);

            // wait for completion, then finish job
            job.finished.whenComplete((r, ex) -> finishJob(jobId, bucket));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ========================================================================
    // BUILD SUMMARY + SEND TO CLIENT
    // ========================================================================
    private void finishJob(String jobId, String bucket) {

        try {
            Job job = jobs.get(jobId);
            if (job == null) return;

            String html = HtmlBuilder.build(job.results);
            Path out = Path.of("/tmp/" + jobId + ".html");

            java.nio.file.Files.writeString(out, html);

            String summaryKey = "summaries/" + jobId + ".html";
            S3Helper.uploadFile(bucket, summaryKey, out);

            String msg = MessageFormatter.formatSummaryDone(jobId, summaryKey);
            SqsHelper.sendMessage(job.callbackQueue, msg);

            System.out.println("[Manager] Summary sent for job " + jobId);

            jobs.remove(jobId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ========================================================================
    // TERMINATE
    // ========================================================================
    private void handleTerminate(Message msg) {

        acceptingNewTasks = false;

        if (!jobs.isEmpty()) {
            CompletableFuture.allOf(
                    jobs.values().stream()
                            .map(j -> j.finished)
                            .toArray(CompletableFuture[]::new)
            ).join();
        }

        Ec2Helper.terminateAllWorkers();
        System.out.println("[Manager] All workers terminated. Manager shutting down.");

        jobPool.shutdown();
        System.exit(0);
    }
}
