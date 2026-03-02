package com.tws.download.constant;

public enum TargetType {
    MODEL("模型"),
    AGENT("代理人");

    private final String description;
    TargetType(String description) { this.description = description; }
    public String getDescription() { return description; }
}