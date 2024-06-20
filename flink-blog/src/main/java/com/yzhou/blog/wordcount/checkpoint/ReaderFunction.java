package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class ReaderFunction extends KeyedStateReaderFunction<String, KeyedState> {
    private ValueState<Long> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> stateDescriptor = new
                ValueStateDescriptor<>("Keyed Aggregation", Types.LONG);
        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void readKey(String key, Context ctx, Collector<KeyedState> out) throws Exception {
        KeyedState data = new KeyedState();
        data.key = key;
        data.value = state.value();
        out.collect(data);
    }
}

