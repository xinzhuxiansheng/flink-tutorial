package com.yzhou.job;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class PKRedisSink extends RichSinkFunction<Tuple3<String, String, Long>>{

    Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("hadoop000", 16379);
        jedis.select(7);
    }

    @Override
    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
        if(!jedis.isConnected()) {
            jedis.connect();
        }

        // 第一个参数 该如何使用呢   pk-product-access-yyyymmdd
        jedis.hset(value.f0, value.f1, value.f2.toString());

    }
    @Override
    public void close() throws Exception {
        if(null != jedis) jedis.close();
    }
}
