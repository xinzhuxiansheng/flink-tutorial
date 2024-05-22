package com.yzhou.flink.example.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TimeUtils;

public class CLI extends ExecutionConfig.GlobalJobParameters {
    public static final String INPUT_KEY = "input";

    public static final String OUTPUT_KEY = "output";

    public static final String DISCOVERY_INTERVAL = "discovery-interval";

    public static final String EXECUTION_MODE = "execution-mode";

    private final Path[] inputs;

    private final Path output;

    private final Duration discoveryInterval;

    private final RuntimeExecutionMode executionMode;

    private final MultipleParameterTool params;

    public static CLI fromArgs(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Path[] inputs = null;
        if (params.has("input")) {
            inputs = (Path[])params.getMultiParameterRequired("input").stream().map(Path::new).toArray(x$0 -> new Path[x$0]);
        } else {
            System.out.println("Executing example with default input data.");
            System.out.println("Use --input to specify file input.");
        }
        Path output = null;
        if (params.has("output")) {
            output = new Path(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
        }
        Duration watchInterval = null;
        if (params.has("discovery-interval"))
            watchInterval = TimeUtils.parseDuration(params.get("discovery-interval"));
        RuntimeExecutionMode executionMode = (RuntimeExecutionMode) ExecutionOptions.RUNTIME_MODE.defaultValue();
        if (params.has("execution-mode"))
            executionMode = RuntimeExecutionMode.valueOf(params.get("execution-mode").toUpperCase());
        return new CLI(inputs, output, watchInterval, executionMode, params);
    }

    private CLI(Path[] inputs, Path output, Duration discoveryInterval, RuntimeExecutionMode executionMode, MultipleParameterTool params) {
        this.inputs = inputs;
        this.output = output;
        this.discoveryInterval = discoveryInterval;
        this.executionMode = executionMode;
        this.params = params;
    }

    public Optional<Path[]> getInputs() {
        return (Optional) Optional.ofNullable(this.inputs);
    }

    public Optional<Duration> getDiscoveryInterval() {
        return Optional.ofNullable(this.discoveryInterval);
    }

    public Optional<Path> getOutput() {
        return Optional.ofNullable(this.output);
    }

    public RuntimeExecutionMode getExecutionMode() {
        return this.executionMode;
    }

    public OptionalInt getInt(String key) {
        if (this.params.has(key))
            return OptionalInt.of(this.params.getInt(key));
        return OptionalInt.empty();
    }

    public Map<String, String> toMap() {
        return this.params.toMap();
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        CLI cli = (CLI)o;
        return (Arrays.equals((Object[])this.inputs, (Object[])cli.inputs) &&
                Objects.equals(this.output, cli.output) &&
                Objects.equals(this.discoveryInterval, cli.discoveryInterval));
    }

    public int hashCode() {
        int result = Objects.hash(new Object[] { this.output, this.discoveryInterval });
        result = 31 * result + Arrays.hashCode((Object[])this.inputs);
        return result;
    }
}
