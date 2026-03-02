package com.tws.download.controller;

import com.tws.download.dto.DownloadRequest;
import com.tws.download.dto.DownloadResponse;
import com.tws.download.service.DownloadService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/download")
public class DownloadController {

    private static final Logger logger = LoggerFactory.getLogger(DownloadController.class);

    private final DownloadService downloadService;

    public DownloadController(DownloadService downloadService) {
        this.downloadService = downloadService;
    }

    @PostMapping("/sign")
    public ResponseEntity<?> signDownload(@Valid @RequestBody DownloadRequest request) {
        try {
            logger.info("Received download request for file: {} with API Key: {}", request.getFileName(),
                    request.getApiKey());

            downloadService.validateApiKey(request.getApiKey());

            DownloadResponse response = downloadService.generateDownloadLink(request.getFileName());
            logger.info("Successfully generated presigned URL for file: {}", request.getFileName());
            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            logger.warn("Unauthorized access attempt: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Unauthorized: " + e.getMessage()));
        } catch (Exception e) {
            logger.error("Error generating download link", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal Server Error"));
        }
    }
}
