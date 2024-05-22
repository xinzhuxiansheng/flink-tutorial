package com.yzhou.job.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LoginWarning extends KeyedProcessFunction {
    @Override
    public void processElement(Object o, Context context, Collector collector) throws Exception {

    }
}
