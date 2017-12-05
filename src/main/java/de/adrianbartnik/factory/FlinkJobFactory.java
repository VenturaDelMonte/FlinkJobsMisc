package de.adrianbartnik.factory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJobFactory<T> {

    private static final int CHECKPOINTING_INTERVAL = 500;

    private final String[] arguments;

    private final boolean chaining;
    private final boolean checkpointing;

    public FlinkJobFactory(String arguments[], boolean chaining, boolean checkpointing) {
        this.arguments = arguments;
        this.checkpointing = checkpointing;
        this.chaining = chaining;
    }

    public StreamExecutionEnvironment createJob(SourceCreator<T> sourceCreator,
                                                JobCreator<T> jobCreator,
                                                SinkCreator<T> sinkCreator) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!chaining) {
            executionEnvironment.disableOperatorChaining();
        }

        if (checkpointing) {
            executionEnvironment.enableCheckpointing(CHECKPOINTING_INTERVAL);
            executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(50);
        }

        DataStream<T> source = sourceCreator.createSource(arguments, executionEnvironment);

        DataStream<T> stream = jobCreator.addOperators(arguments, source);
        sinkCreator.addSink(arguments, stream);

        return executionEnvironment;
    }

    public interface SourceCreator<T> {
        DataStream<T> createSource(String arguments[], StreamExecutionEnvironment executionEnvironment);
    }

    public interface JobCreator<T> {
        DataStream<T> addOperators(String arguments[], DataStream<T> dataSource);
    }

    public interface SinkCreator<T> {
        void addSink(String arguments[], DataStream<T> dataSource);
    }
}
