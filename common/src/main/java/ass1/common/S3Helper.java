package ass1.common;

import java.nio.file.Path;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Helper {
    private static final S3Client s3 = AWS.getInstance().s3();

    public static  void uploadFile(String bucketName, String key, Path filePath) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(request, filePath);
        System.out.printf("[S3] Uploaded: %s → s3://%s/%s%n", filePath, bucketName, key);
    }

    public static  void downloadFile(String bucketName, String key, Path destination) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.getObject(request, destination);
        System.out.printf("[S3] Downloaded: s3://%s/%s → %s%n", bucketName, key, destination);
    }

    public static  boolean fileExists(String bucketName, String key) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3.headObject(request);
            return true;
        } catch (S3Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}

