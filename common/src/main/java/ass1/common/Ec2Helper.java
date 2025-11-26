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
    private static final String ami = "ami-00e95a9222311e8ed"; 
    private static final String keyName = "vockey"; // חשוב שיהיה קיים
    private static final String iamProfile = "LabInstanceProfile";
    private static final String workerTagName = "Worker";

    private static final String MANAGER_USER_DATA =
        "#!/bin/bash\n" +
        "sudo apt-get update -y\n" +
        "sudo apt-get install -y default-jdk\n" +
        "aws s3 cp s3://your-bucket/manager.jar /home/ubuntu/manager.jar\n" +
        "java -jar /home/ubuntu/manager.jar > /home/ubuntu/manager.log 2>&1";

    public static boolean isManagerRunning() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Name").values(managerTagName).build(),
                        Filter.builder().name("instance-state-name").values("pending", "running").build()
                ).build();

        DescribeInstancesResponse response = ec2.describeInstances(request);
        return response.reservations().stream() 
                .anyMatch(res -> !res.instances().isEmpty());
    }

    public static String startManagerInstance() {
        String bucket = AWS.getInstance().bucketName;

        String userData =
                "#!/bin/bash\n" +
                "sudo apt-get update -y\n" +
                "sudo apt-get install -y default-jdk awscli\n" +
                "aws s3 cp s3://" + bucket + "/manager/manager.jar /home/ubuntu/manager.jar\n" +
                "java -jar /home/ubuntu/manager.jar > /home/ubuntu/manager.log 2>&1\n";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(1)
                .minCount(1)
                .keyName(keyName)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(iamProfile).build())
                .userData(Base64.getEncoder().encodeToString(userData.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(Tag.builder().key("Name").value(managerTagName).build())
                .build();

        ec2.createTags(tagRequest);

        System.out.println("[EC2] Manager instance created: " + instanceId);
        return instanceId;
        }


    public static void createWorkersIfNeeded(int requiredCount) {
        // 1. Count currently running workers
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Name").values("Worker").build(),
                        Filter.builder().name("instance-state-name")
                                .values("running", "pending").build()
                ).build();

        List<Instance> workers = ec2.describeInstances(request)
                .reservations().stream()
                .flatMap(res -> res.instances().stream())
                .toList();

        int running = workers.size();
        int missing = requiredCount - running;

        if (missing <= 0) {
                System.out.println("[EC2] Enough workers running (" + running + ")");
                return;
        }

        // Respect the limit of 19 total workers
        missing = Math.min(missing, 19 - running);
        if (missing <= 0) {
                System.out.println("[EC2] Worker limit reached (19). Not creating more.");
                return;
        }

        System.out.println("[EC2] Creating " + missing + " worker(s)");

        for (int i = 0; i < missing; i++) {
                startWorkerInstance();
        }
        }

     public static String startWorkerInstance() {
        String userData = Base64.getEncoder().encodeToString(
                ("#!/bin/bash\n" +
                "sudo apt-get update -y\n" +
                "sudo apt-get install -y default-jdk\n" +
                "aws s3 cp s3://your-bucket/worker.jar /home/ubuntu/worker.jar\n" +
                "java -jar /home/ubuntu/worker.jar > /home/ubuntu/worker.log 2>&1")
                .getBytes()
        );

        RunInstancesRequest runReq = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(1)
                .keyName(keyName)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(iamProfile).build())
                .userData(userData)
                .build();

        RunInstancesResponse res = ec2.runInstances(runReq);
        String id = res.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder()
                .resources(id)
                .tags(Tag.builder().key("Name").value(workerTagName).build())
                .build());

        System.out.println("[EC2] Worker instance created: " + id);
        return id;
        }

    public static void terminateAllWorkers() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Name").values("Worker").build(),
                        Filter.builder().name("instance-state-name").values("running", "pending").build()
                ).build();

        List<Instance> workers = ec2.describeInstances(request)
                .reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();

        if (!workers.isEmpty()) {
            List<String> ids = workers.stream().map(Instance::instanceId).toList();
            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(ids).build();
            ec2.terminateInstances(terminateRequest);
            System.out.println("[EC2] Terminated " + ids.size() + " worker(s).");
        }
    }
}
