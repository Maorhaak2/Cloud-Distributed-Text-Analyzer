package ass1.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

public class S3Helper {

    private static final S3Client s3 = AWS.getInstance().s3();

    // Upload
    public static void uploadFile(String bucketName, String key, Path filePath) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(request, filePath);
        System.out.printf("[S3] Uploaded: %s → s3://%s/%s%n", filePath, bucketName, key);
    }

    // Download
    public static void downloadFile(String bucket, String key, Path dest) {
        try {
            if (Files.exists(dest)) {
                Files.delete(dest);
            }

            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build(),
                ResponseTransformer.toFile(dest)
            );

        } catch (IOException e) {
            throw new RuntimeException("Failed to download file", e);
        }
    }

    // Check if exists
    public static boolean fileExists(String bucketName, String key) {
        try {
            s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()
            );
            return true;

        } catch (S3Exception e) {
            return false;
        }
    }

    // Create bucket
    public static void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(
                CreateBucketRequest.builder().bucket(bucketName).build()
            );

            s3.waiter().waitUntilBucketExists(
                HeadBucketRequest.builder().bucket(bucketName).build()
            );

            System.out.println("[S3] Bucket created: " + bucketName);

        } catch (S3Exception e) {

            if (!e.awsErrorDetails().errorMessage().contains("BucketAlreadyOwnedByYou")) {
                throw e;
            }

            System.out.println("[S3] Bucket already exists: " + bucketName);
        }
    }



    public static String generatePresignedUrl(String bucketName, String key) {
    try {
        S3Presigner presigner = S3Presigner.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(
                GetObjectPresignRequest.builder()
                        .signatureDuration(Duration.ofDays(7))
                        .getObjectRequest(getObjectRequest)
                        .build()
        );

        presigner.close();
        return presignedRequest.url().toString();

    } catch (Exception e) {
        System.err.println("[S3Helper] Failed to generate presigned URL: " + e.getMessage());
        return null;
    }
}

}
