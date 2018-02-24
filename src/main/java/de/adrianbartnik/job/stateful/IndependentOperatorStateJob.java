package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IndependentOperatorStateJob {

    private static final String JOB_NAME = "IndependentOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int maxNumberOfMessages = params.getInt("maxNumberOfMessages", 100_000);
        final int pauseDuration = params.getInt("pauseDuration", 50);
        final int sourceParallelism = params.getInt("sourceParallelism", 3);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String external_backend = params.get("backend", "memory");
        final String backendPath = params.get("backendPath", "file://tmp/");
        final boolean asynchronousCheckpoints = params.getBoolean("asynchronousCheckpoints", true);
        final boolean incrementalCheckpoints = params.getBoolean("incrementalCheckpoints", true);
        final int checkpointingInterval = params.getInt("checkpointingInterval", 1000);
        final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 2000);

        StreamExecutionEnvironment env = new FlinkJobFactory<>(args, false, true)
                .setupExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointInterval(checkpointingInterval);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        switch (external_backend) {
            case "memory":
                env.setStateBackend(new MemoryStateBackend());
                break;
            case "filesystem":
                env.setStateBackend(new FsStateBackend(backendPath, asynchronousCheckpoints));
                break;
            case "rocksdb":
                env.setStateBackend(new RocksDBStateBackend(backendPath, incrementalCheckpoints));
                break;
            default:
                throw new IllegalArgumentException();
        }

        DataStream<Long> source = new IntervalSequenceSource(0, maxNumberOfMessages, pauseDuration, sourceParallelism)
                .createSource(args, env);

        DataStream<String> coutingMap = new CountingMap<Long>(mapParallelism).createOperator(args, source);

        new TextOutputSink<String>(sinkParallelism).createSink(args, coutingMap);

        env.execute(JOB_NAME);
    }
}