package com.yzhou.flink.common.exception;

import com.yzhou.flink.common.exception.enums.BizExceptionInfo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BizRuntimeException extends RuntimeException {

    /**
     * author: Imooc
     * description: 自定义异常类构造方法
     * @param info:  自定义异常枚举对象
     * @return null
     */
    public BizRuntimeException(BizExceptionInfo info) {

        log.error(info.getExceptionMsg());
    }
}
