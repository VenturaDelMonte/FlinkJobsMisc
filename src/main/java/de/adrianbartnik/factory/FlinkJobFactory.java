package de.adrianbartnik.factory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJobFactory<INPUT, OUTPUT> {

    private static final int CHECKPOINTING_INTERVAL = 500;

    private final String[] arguments;

    private final boolean chaining;
    private final boolean checkpointing;

    public FlinkJobFactory(String arguments[], boolean chaining, boolean checkpointing) {
        this.arguments = arguments;
        this.checkpointing = checkpointing;
        this.chaining = chaining;
    }

    public StreamExecutionEnvironment createJob(SourceCreator<INPUT> sourceCreator,
                                                JobCreator<INPUT, OUTPUT> jobCreator,
                                                SinkCreator<OUTPUT> sinkCreator) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!chaining) {
            executionEnvironment.disableOperatorChaining();
        }

        if (checkpointing) {
            executionEnvironment.enableCheckpointing(CHECKPOINTING_INTERVAL);
            executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(50);
        }

        DataStream<INPUT> source = sourceCreator.createSource(arguments, executionEnvironment);

        DataStream<OUTPUT> stream = jobCreator.addOperators(arguments, source);
        sinkCreator.addSink(arguments, stream);

        return executionEnvironment;
    }

    public interface SourceCreator<T> {
        DataStream<T> createSource(String arguments[], StreamExecutionEnvironment executionEnvironment);
    }

    public interface JobCreator<IN, OUT> {
        DataStream<OUT> addOperators(String arguments[], DataStream<IN> dataSource);
    }

    public interface SinkCreator<T> {
        void addSink(String arguments[], DataStream<T> dataSource);
    }
}
