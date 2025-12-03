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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
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
    private volatile boolean shouldTerminate = false;

    private AtomicInteger currentRunningWorkers = new AtomicInteger(0);


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
        final CompletableFuture<Void> finished =    new CompletableFuture<>();

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
    // LISTENER: LOCALAPP INPUT (CLEAN VERSION)
    // ========================================================================
    private void startLocalAppListener() {
        Thread t = new Thread(() -> {
            System.out.println("[Manager] LocalApp Listener started.");

            while (acceptingNewTasks || !jobs.isEmpty()) {

                try {
                    List<Message> messages =
                            SqsHelper.receiveMessages(managerInputQueueUrl, 10);

                    for (Message msg : messages) {

                        MessageType type = MessageFormatter.getMessageType(msg.body());

                        // אין טיפוסים אחרים — הכל NEW_TASK
                        if (type == MessageType.NEW_TASK) {

                            if (acceptingNewTasks) {
                                System.out.println("[Manager] NEW_TASK received");
                                jobPool.submit(() -> handleNewTask(msg));
                            } else {
                                System.out.println("[Manager] Ignoring NEW_TASK (termination mode)");
                            }

                        } else {
                            System.out.println("[Manager] Ignored unexpected message type: " + type);
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

            while (!jobs.isEmpty() || acceptingNewTasks) {
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

    MessageType type = MessageFormatter.getMessageType(body);

    switch (type) {

        // -------------------------------------------------------
        // WORKER DONE (success)
        // -------------------------------------------------------
        case WORKER_DONE -> {
            MessageFormatter.WorkerDoneFields f = MessageFormatter.parseWorkerDone(body);

            Job job = jobs.get(f.jobId());
            if (job == null) return;

            String jobKey = UUID.randomUUID().toString();
            String value = f.analysisType() + "\t" + f.url() + "\t" + f.resultInfo();

            job.results.put(jobKey, value);

            int done = job.completed.incrementAndGet();
            System.out.println("[Manager] Job " + job.jobId + ": " + done + "/" + job.totalTasks);

            if (done == job.totalTasks) {
                job.finished.complete(null);
            }
        }

        // -------------------------------------------------------
        // WORKER ERROR (exception in worker)
        // -------------------------------------------------------
        case WORKER_ERROR -> {
            MessageFormatter.WorkerErrorFields f = MessageFormatter.parseWorkerError(body);

            Job job = jobs.get(f.jobId());
            if (job == null) return;

            // HTML format expects:
            // <analysis>: <inputUrl> <short error text>
            String jobKey = UUID.randomUUID().toString();
            String value = f.analysisType() + "\t" + f.url() + "\t" + ("ERROR: " + f.errorMsg());

            job.results.put(jobKey, value);

            int done = job.completed.incrementAndGet();
            System.out.println("[Manager] ERROR reported for job " + job.jobId
                    + " (" + done + "/" + job.totalTasks + ")");

            if (done == job.totalTasks) {
                job.finished.complete(null);
            }
        }

        default -> {
            System.out.println("[Manager] Unexpected message type: " + type);
        }
    }
}



    // ========================================================================
    // HANDLER: NEW TASK
    // ========================================================================
    private void handleNewTask(Message msg) {

        try {
            MessageFormatter.NewTaskFields f = MessageFormatter.parseNewTask(msg.body());
            if (shouldTerminate) {
                System.out.println("[Manager] NEW_TASK ignored — TERMINATE already requested.");
                return;
            }

            String fullS3 = f.inputFileS3();
            String bucket = AWS.getInstance().bucketName;
            String key = fullS3.substring(("s3://" + bucket + "/").length());

            Path temp = Path.of("/tmp/" + UUID.randomUUID() + ".txt");
            S3Helper.downloadFile(bucket, key, temp);

            List<String> lines = java.nio.file.Files.readAllLines(temp);

            // Keep only non-empty lines
            List<String> nonEmptyLines = lines.stream()
                    .map(String::trim)
                    .filter(l -> !l.isEmpty())
                    .toList();

            // Create job
            String jobId = UUID.randomUUID().toString();
            Job job = new Job(jobId, f.callbackQueue(), nonEmptyLines.size());
            jobs.put(jobId, job);

            System.out.println("[Manager] Job created. " + nonEmptyLines.size() + " tasks.");

            // Send tasks to workers
            for (String line : nonEmptyLines) {
                String[] p = line.split("\t");
                String analysisType = p[0];
                String url = p[1];

                String msgOut = MessageFormatter.formatAnalyzeTask(analysisType, url, jobId);
                SqsHelper.sendMessage(workerTasksQueueUrl, msgOut);
            }

            // Create workers if needed
            int neededWorkers = (int) Math.ceil(nonEmptyLines.size() / (double) f.n());
            int requiredWorkers = Math.max(neededWorkers - currentRunningWorkers.get(), 0);

            requiredWorkers = Math.min(18 - currentRunningWorkers.get(), requiredWorkers);
            if (requiredWorkers > 0) {
                currentRunningWorkers.addAndGet(requiredWorkers);
                System.out.println("[Manager] Creating " + requiredWorkers + " new worker(s).");
                Ec2Helper.createWorkers(requiredWorkers);
            }
            else {
                System.out.println("[EC2] Enough workers running. Currently: " + currentRunningWorkers.get());
            }

            if(f.terminate()){
                shouldTerminate = true;
                acceptingNewTasks = false;
                System.out.println("[Manager] TERMINATE flag activated — no more NEW_TASK will be accepted.");
            }

            // Wait for completion then finish job
            job.finished.whenComplete((r, ex) -> {
                finishJob(jobId, bucket);
            });

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

            if(shouldTerminate && jobs.isEmpty()){
                shutdownManager();
            }

            

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ========================================================================
    // TERMINATE
    // ========================================================================

    private void shutdownManager() {
        System.out.println("[Manager] TERMINATE — waiting for all running jobs...");

        Ec2Helper.terminateAllWorkers();
        System.out.println("[Manager] All workers terminated.");

        try {
            String instanceId = EC2MetadataUtils.getInstanceId();
            System.out.println("[Manager] Self instance id: " + instanceId);

            Ec2Client ec2 = Ec2Client.builder()
                    .region(Region.US_EAST_1)
                    .build();

            ec2.terminateInstances(
                    TerminateInstancesRequest.builder()
                            .instanceIds(instanceId)
                            .build()
            );

            System.out.println("[Manager] Termination request sent for Manager instance.");
            ec2.close();
        } catch (Exception e) {
            System.err.println("[Manager] Failed to self-terminate: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[Manager] Manager shutting down.");
        System.exit(0);
    }


}
