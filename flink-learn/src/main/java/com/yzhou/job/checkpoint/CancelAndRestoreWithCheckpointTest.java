package com.yzhou.job.checkpoint;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class CancelAndRestoreWithCheckpointTest {
    private static final boolean ENABLE_INCREMENTAL_CHECKPOINT = true;
    private static final int NUMBER_OF_TRANSFER_THREADS = 3;

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        // configuration.setString("execution.savepoint.path", "file:///Users/a/TMP/flink_savepoint");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // ck 设置
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(3));
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3L);

        env.configure(configuration, Thread.currentThread().getContextClassLoader());

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 状态后端设置
        // 设置存储文件位置为 file:///Users/flink/checkpoints
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(ENABLE_INCREMENTAL_CHECKPOINT);
        rocksDBStateBackend.setNumberOfTransferThreads(NUMBER_OF_TRANSFER_THREADS);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        rocksDBStateBackend.setDbStoragePath("file:///Users/a/TMP/flink_checkpoint");
        env.setStateBackend((StateBackend) rocksDBStateBackend);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(6, org.apache.flink.api.common.time.Time
                .of(10L, TimeUnit.MINUTES), org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(10);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.enabled", "true");
        tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "60 s");

        String sqlStr = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/checkpoint/testcheckpoint.sql");

        tEnv.getConfig().getConfiguration().setString("pipeline.name", "1.15.4 WINDOW TVF TUMBLE WINDOW EARLY FIRE 案例");

         Arrays.stream(sqlStr.split(";")).forEach(tEnv::executeSql);
    }
}
