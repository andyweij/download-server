package com.tws.download.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.List;

@Data
@SuperBuilder
@NoArgsConstructor
public class DownloadResponse {
    private String objectName;
    private Instant expiresAt;
    private List<DownloadInfoDTO> files;
    private int filesCount;
}
