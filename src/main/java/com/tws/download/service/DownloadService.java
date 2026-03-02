package com.tws.download.service;

import com.tws.download.component.AWSS3Util;
import com.tws.download.dto.DownloadInfoDTO;
import com.tws.download.dto.DownloadResponse;
import com.tws.download.dto.S3ObjectInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class DownloadService {

    private final AWSS3Util awsS3Util;

    // 定義過期時間常數，方便未來管理
    private static final Duration URL_EXPIRATION = Duration.ofHours(1);

    public DownloadService(AWSS3Util awsS3Util) {
        this.awsS3Util = awsS3Util;
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

    public DownloadResponse generateDownloadLink(String fileName) {
        // 1. 決定過期時間點
        Instant expiresAt = Instant.now().plus(URL_EXPIRATION);

        // 2. 呼叫底層工具取得內部 S3 模型列表
        List<S3ObjectInfo> s3Objects = awsS3Util.generateObjectDownloadLinks(fileName, URL_EXPIRATION);

        // 3. 轉換為對外的 DTO (DownloadInfoDTO)
        List<DownloadInfoDTO> fileDtos = s3Objects.stream()
                .map(info -> DownloadInfoDTO.builder()
                        .relativePath(info.getRelativePath())
                        .size(info.getSize())
                        .downloadUrl(info.getDownloadUrl())
                        .build())
                .collect(Collectors.toList());

        // 4. 封裝並回傳最終的 Response DTO
        return DownloadResponse.builder()
                .objectName(fileName)
                .expiresAt(expiresAt)
                .files(fileDtos)
                .filesCount(fileDtos.size())
                .build();
    }
}
