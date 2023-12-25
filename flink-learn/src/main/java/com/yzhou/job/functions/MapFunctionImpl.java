package com.yzhou.job.functions;

import com.yzhou.job.transform.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 自定义 MapFunction
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
