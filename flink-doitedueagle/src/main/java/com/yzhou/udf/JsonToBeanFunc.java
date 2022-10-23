package com.yzhou.udf;

import com.alibaba.fastjson.JSON;
import com.yzhou.pojo.DataBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yzhou
 * @date 2022/10/22
 */
public class JsonToBeanFunc extends ProcessFunction<String, DataBean> {
    @Override
    public void processElement(String value, ProcessFunction<String, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        try {
            DataBean dataBean = JSON.parseObject(value, DataBean.class);
            out.collect(dataBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
