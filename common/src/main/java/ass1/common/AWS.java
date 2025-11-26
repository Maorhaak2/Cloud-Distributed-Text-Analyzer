package ass1.common;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

public class AWS {
    private static final AWS instance = new AWS();
    public static final String bucketName = "text_jobs_bucket"; 
    public static final Region region = Region.US_EAST_1;
    
    private final S3Client s3 = S3Client.builder()
            .region(region)
            .build();

    private final SqsClient sqs = SqsClient.builder()
            .region(region)
            .build();

    private final Ec2Client ec2 = Ec2Client.builder()
            .region(region)
            .build();

    private AWS() {
        System.out.println("[AWS] Initialized clients for region: " + region);
    }

    public static AWS getInstance() {
        return instance;
    }

    public S3Client s3() {
        return s3;
    }

    public SqsClient sqs() {
        return sqs;
    }

    public Ec2Client ec2() {
        return ec2;
    }
}
