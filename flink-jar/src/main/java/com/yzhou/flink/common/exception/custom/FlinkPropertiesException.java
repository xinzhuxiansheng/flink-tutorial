package com.yzhou.flink.common.exception.custom;

import com.yzhou.flink.common.exception.BizRuntimeException;
import com.yzhou.flink.common.exception.enums.BizExceptionInfo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkPropertiesException extends BizRuntimeException {
    public FlinkPropertiesException(BizExceptionInfo info) {
        super(info);
    }
}