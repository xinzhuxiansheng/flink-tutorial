package com.yzhou.flink.common.exception.enums;

public interface BizExceptionInfo {

    /**
     * description: 获取异常错误码
     * @param :
     * @return java.lang.String
     */
    String getExceptionCode();

    /**
     * description: 获取异常信息
     * @param :
     * @return java.lang.String
     */
    String getExceptionMsg();
}