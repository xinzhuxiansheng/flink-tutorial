package com.yzhou.udf;

import com.yzhou.pojo.DataBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yzhou
 * @date 2022/10/24
 * <p>
 * 根据设备udid判断用户是不是新用户
 * 先按照设备类型进行KeyBy
 */
public class IsNewUserFunction extends KeyedProcessFunction<String, DataBean, DataBean> {

    private transient ValueState<BloomFilter<String>> bloomFilterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<BloomFilter<String>> valueStateDescriptor = new ValueStateDescriptor<>("uid-bloom-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
        }));
        bloomFilterState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(DataBean value, KeyedProcessFunction<String, DataBean, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {

        String deviceId = value.getDeviceId();
        BloomFilter<String> bloomFilter = bloomFilterState.value();
        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
        }
        // 判断该设备是否在布隆过滤器中
        if (!bloomFilter.mightContain(deviceId)) {
            bloomFilter.put(deviceId);
            value.setIsN(1); // 是新用户
            bloomFilterState.update(bloomFilter);
        }
        out.collect(value);
    }
}
