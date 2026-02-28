package com.tws.download.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.time.Duration;

@Service
public class DownloadService {

    private final S3Presigner s3Presigner;
    private final String bucketName;

    public DownloadService(S3Presigner s3Presigner, @Value("${aws.s3.bucket}") String bucketName) {
        this.s3Presigner = s3Presigner;
        this.bucketName = bucketName;
    }

    /**
     * Validates the API key.
     * Throws an exception if invalid.
     *
     * @param apiKey The API Key to validate
     */
    public void validateApiKey(String apiKey) {
        // Mock validation logic
        // In a real scenario, check against DB or another service
        if (apiKey == null || apiKey.trim().isEmpty() || !"valid-mock-key".equals(apiKey)) {
            // Throwing RuntimeException for simplicity, captured by Controller
            throw new IllegalArgumentException("Invalid API Key");
        }
    }

    /**
     * Generates a presigned URL for the given file.
     *
     * @param fileName The key of the file in S3
     * @return The presigned URL request containing the URL and expiration
     */
    public PresignedGetObjectRequest generateDownloadLink(String fileName) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key("models/" + fileName)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofHours(1))
                .getObjectRequest(objectRequest)
                .build();

        return s3Presigner.presignGetObject(presignRequest);
    }
}
