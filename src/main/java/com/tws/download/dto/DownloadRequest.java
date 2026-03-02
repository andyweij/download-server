package com.tws.download.dto;

import com.tws.download.constant.TargetType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DownloadRequest {
    @NotBlank
    private String apiKey;
    @NotNull
    private TargetType targetType;
    @NotBlank
    private String fileName;
}
