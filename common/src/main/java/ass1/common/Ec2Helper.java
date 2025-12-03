package ass1.common;

import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

public class Ec2Helper {

    private static final Ec2Client ec2 = AWS.getInstance().ec2();

    private static final String managerTagName = "Manager";
    private static final String workerTagName  = "Worker";
    private static final String ami = "ami-0c398cb65a93047f2"; 
    private static final String keyName = "vockey"; 
    private static final String iamProfile = "LabInstanceProfile"; 
    
    private static final InstanceType MANAGER_INSTANCE_TYPE = InstanceType.T3_MICRO;
    //private static final InstanceType WORKER_INSTANCE_TYPE = InstanceType.T3_MEDIUM;


    // ---------------------------------------------------------
    //  USER DATA BUILDER — משותף למנג'ר ולוורקר
    // ---------------------------------------------------------
    private static String buildUserData(String jarS3Path, String outputJarName) {
        String bucket = AWS.getInstance().bucketName;

        String script =
                "#!/bin/bash\n" +
                "sudo apt-get update -y\n" +
                "sudo apt-get install -y openjdk-17-jre-headless unzip curl\n" +

                "# Install AWS CLI v2\n" +
                "curl \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o \"awscliv2.zip\"\n" +
                "unzip awscliv2.zip\n" +
                "sudo ./aws/install\n" +

                "mkdir -p /home/ubuntu\n" +
                "cd /home/ubuntu\n" +

                "aws s3 cp s3://" + bucket + "/" + jarS3Path + " " + outputJarName + "\n" +

                "nohup java -jar " + outputJarName + " > run.log 2>&1 &\n";

        return Base64.getEncoder().encodeToString(script.getBytes());
    }


    public static boolean isManagerRunning() {
        List<Instance> managers = getRunningInstances("Manager");

        return !managers.isEmpty();
    }

    public static List<Instance> getRunningInstances(String tagName) {
        DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Name").values(tagName).build(),
                        Filter.builder().name("instance-state-name")
                                .values("pending", "running").build()
                )
                .build();

        DescribeInstancesResponse res = ec2.describeInstances(req);

        return res.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }


    // ---------------------------------------------------------
    //  START MANAGER
    // ---------------------------------------------------------
    public static String startManagerInstance() {

        String userData = buildUserData("manager/manager.jar", "manager.jar");

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(MANAGER_INSTANCE_TYPE)
                .minCount(1)
                .maxCount(1)
                .keyName(keyName)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(iamProfile).build())
                .userData(userData)
                .build();

        RunInstancesResponse res = ec2.runInstances(runRequest);
        String instanceId = res.instances().get(0).instanceId();

        // Tag the instance
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(Tag.builder().key("Name").value(managerTagName).build())
                .build();
        ec2.createTags(tagRequest);

        System.out.println("[EC2] Manager instance created: " + instanceId);
        return instanceId;
    }

    // ---------------------------------------------------------
    //  START WORKER
    // ---------------------------------------------------------
    public static String startWorkerInstance() {
        String userData = buildUserData("worker/worker.jar", "worker.jar");

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(MANAGER_INSTANCE_TYPE)
                .minCount(1)
                .maxCount(1)
                .keyName(keyName)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(iamProfile).build())
                .userData(userData)
                .build();

        RunInstancesResponse res = ec2.runInstances(runRequest);
        String id = res.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder()
                .resources(id)
                .tags(Tag.builder().key("Name").value(workerTagName).build())
                .build());

        System.out.println("[EC2] Worker instance created: " + id);
        return id;
    }

    // ---------------------------------------------------------
    //  CREATE WORKERS ONLY IF NEEDED
    // ---------------------------------------------------------
    public static void createWorkers(int requiredCount) {

        System.out.println("[EC2] Creating " + requiredCount + " new worker(s).");
        for (int i = 1; i <= requiredCount; i++) {
            startWorkerInstance();
        }
    }

    // ---------------------------------------------------------
    //  TERMINATE ALL WORKERS
    // ---------------------------------------------------------
    public static void terminateAllWorkers() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Name").values(workerTagName).build(),
                        Filter.builder().name("instance-state-name").values(
                                "running","pending"
                        ).build()
                ).build();

        List<Instance> workers = ec2.describeInstances(request)
                .reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();

        if (workers.isEmpty()) {
            System.out.println("[EC2] No workers to terminate.");
            return;
        }

        List<String> ids = workers.stream().map(Instance::instanceId).toList();

        TerminateInstancesRequest termReq = TerminateInstancesRequest.builder()
                .instanceIds(ids)
                .build();

        ec2.terminateInstances(termReq);
        System.out.println("[EC2] Terminated " + ids.size() + " workers.");
    }
}