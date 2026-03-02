package com.tws.download.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class S3ObjectInfo {
    private String relativePath;
    private long size;
    private String downloadUrl;
}