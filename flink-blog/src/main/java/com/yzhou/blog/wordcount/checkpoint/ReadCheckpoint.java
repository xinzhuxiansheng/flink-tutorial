package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class ReadCheckpoint {
    public static void main(String[] args) throws Exception {
        String metadataPath = "D:\\TMP\\7caba09b0eea52c93cbaf809a3a2c2fa\\chk-1";

        //String metadataPath = "D:\\TMP\\c43c2293d311ddc1b6151451bcc95a71\\chk-7";

        // 尝试 一
        CheckpointMetadata metadataOnDisk = SavepointLoader.loadSavepointMetadata(metadataPath);
//        System.out.println("checkpointId: " + metadataOnDisk.getCheckpointId());

        // 尝试 二
//        readState(metadataPath);
        parseCheckpointMetadata02(metadataPath);
    }

    public static void readState(String ckPath) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        bEnv.setParallelism(1);

        ExistingSavepoint savepoint = Savepoint.load(bEnv, ckPath, new HashMapStateBackend());
        // 定义 KeyedStateReaderFunction 读取状态
        DataSet<Tuple2<String, Integer>> keyedCountState = savepoint.readKeyedState(
                "wc-sum",
                new KeyedStateReaderFunction<String, Tuple2<String, Integer>>() {
                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                                "count",
                                Integer.class));
                    }

                    @Override
                    public void readKey(
                            String key,
                            Context ctx,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(key, countState.value()));
                    }
                });
        keyedCountState.print();
    }

    public static void parseCheckpointMetadata01(String metadataPath) throws IOException {
        // 读取检查点元数据
        CheckpointMetadata checkpointMetadata = SavepointLoader.loadSavepointMetadata(metadataPath);
        System.out.println("Checkpoint ID: " + checkpointMetadata.getCheckpointId());
        System.out.println(
                "Number of Operator States: " + checkpointMetadata.getOperatorStates().size());

        // 遍历每个算子的状态
        checkpointMetadata.getOperatorStates().forEach((operatorState) -> {
            System.out.println("Operator ID: " + operatorState.getOperatorID());
            System.out.println("Number of Subtask States: " + operatorState.getStates().size());
        });

    }

    public static void parseCheckpointMetadata02(String metadataPath) throws IOException {
        // 读取检查点元数据
        CheckpointMetadata checkpointMetadata = SavepointLoader.loadSavepointMetadata(metadataPath);
        System.out.println("Checkpoint ID: " + checkpointMetadata.getCheckpointId());
        System.out.println(
                "Number of Operator States: " + checkpointMetadata.getOperatorStates().size());

        // 遍历每个算子的状态
//        checkpointMetadata.getOperatorStates().forEach((operatorState) -> {
//            System.out.println("Operator ID: " + operatorState.getOperatorID());
//            System.out.println("Number of Subtask States: " + operatorState.getStates().size());
//        });

        // 遍历每个操作符的状态
        for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
            System.out.println("Operator ID: " + operatorState.getOperatorID());
            for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
                for (KeyedStateHandle keyedStateHandle : subtaskState
                        .getManagedKeyedState()
                        .asList()) {
                    if (keyedStateHandle instanceof KeyGroupsStateHandle) {
                        KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
                        for (Tuple2<Integer, Long> entry : keyGroupsStateHandle.getGroupRangeOffsets()) {
                            int keyGroup = entry.f0;
                            long offset = entry.f1;

                            // 读取 KeyGroup 的状态数据
                            Path filePath = new Path(metadataPath, "_metadata");
                            try (FSDataInputStream inputStream = filePath
                                    .getFileSystem()
                                    .open(filePath)) {
                                inputStream.seek(offset);
                                DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(
                                        inputStream);
//                                int namespaceId = div.readInt();
//                                int keyId = div.readInt();
//                                long sumValue = div.readLong(); // 读取 sum 的状态值
//
//                                System.out.println("Namespace ID: " + namespaceId);
//                                System.out.println("Key ID: " + keyId);
//                                System.out.println("Sum Value: " + sumValue);

                                // System.out.println("aaa");

                            }
                        }
                    }
                }
            }
        }
    }
}
