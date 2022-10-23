package com.yzhou.datagen;

import lombok.Data;

import java.util.Map;

/**
 * @author yzhou
 * @date 2022/10/23
 */
@Data
public class LogBean {
    private String account        ;
    private String appId          ;
    private String appVersion     ;
    private String carrier        ;
    private String deviceId       ;
    private String deviceType     ;
    private String ip             ;
    private double latitude       ;
    private double longitude      ;
    private String netType        ;
    private String osName         ;
    private String osVersion      ;
    private String releaseChannel ;
    private String resolution     ;
    private String sessionId      ;
    private long timeStamp        ;
    private String eventId        ;
    private Map<String,String> properties;
}
