package com.yzhou.common.data.exception;

public class RunTimeException extends RuntimeException {

    public RunTimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RunTimeException(String message) {
        super(message);
    }
}
