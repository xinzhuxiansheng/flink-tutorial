package com.yzhou.job.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintSink extends RichSinkFunction<String> {
    private Logger logger = LoggerFactory.getLogger(PrintSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("PrintSink open()");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);
        System.out.println("PrintSink invoke()");
    }
}
