package com.yzhou.job.pojo;

import lombok.Data;

@Data
public class Warning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private Long warningMsg;

    public Warning() {
    }
}
