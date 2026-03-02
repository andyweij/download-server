package com.tws.download.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
public class DownloadInfoDTO {
    private String relativePath;
    private long size;
    private String downloadUrl;
}
