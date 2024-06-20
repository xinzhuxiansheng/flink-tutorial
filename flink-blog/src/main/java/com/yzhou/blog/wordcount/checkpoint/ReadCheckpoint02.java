package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadCheckpoint02 {
    public static void main(String[] args) throws Exception {
        String metadataPath = "D:\\TMP\\7caba09b0eea52c93cbaf809a3a2c2fa\\chk-1";

        //String metadataPath = "D:\\TMP\\c43c2293d311ddc1b6151451bcc95a71\\chk-7";

//        CheckpointMetadata metadataOnDisk = SavepointLoader.loadSavepointMetadata(metadataPath);
//        System.out.println("checkpointId: " + metadataOnDisk.getCheckpointId());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SavepointReader savepoint = SavepointReader.read(env, metadataPath, new HashMapStateBackend());
        // 定义 KeyedStateReaderFunction 读取状态
        DataStream<KeyedState> keyedCountState = savepoint.readKeyedState(
                "wc-sum", new ReaderFunction());
        keyedCountState.print();
        env.execute();
    }
}
