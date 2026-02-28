package com.tws.download.dto;

import java.time.Instant;

public class DownloadResponse {
    private String downloadUrl;
    private Instant expiresAt;

    public DownloadResponse(String downloadUrl, Instant expiresAt) {
        this.downloadUrl = downloadUrl;
        this.expiresAt = expiresAt;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public void setDownloadUrl(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
}
