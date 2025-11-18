package ass1.common;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;

public class S3Helper {
    private final S3Client s3 = AWS.getInstance().s3();

    public void uploadFile(String bucketName, String key, Path filePath) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(request, filePath);
        System.out.printf("[S3] Uploaded: %s → s3://%s/%s%n", filePath, bucketName, key);
    }

    public void downloadFile(String bucketName, String key, Path destination) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.getObject(request, destination);
        System.out.printf("[S3] Downloaded: s3://%s/%s → %s%n", bucketName, key, destination);
    }

    public boolean fileExists(String bucketName, String key) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3.headObject(request);
            return true;
        } catch (NoSuchKeyException | S3Exception e) {
            return false;
        }
    }
}
