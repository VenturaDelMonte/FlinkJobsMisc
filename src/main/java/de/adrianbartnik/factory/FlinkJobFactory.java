package de.adrianbartnik.factory;

import de.adrianbartnik.operator.AbstractOperator;
import de.adrianbartnik.sink.AbstractSink;
import de.adrianbartnik.source.AbstractSource;
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

    public StreamExecutionEnvironment createJob(AbstractSource<INPUT> sourceCreator,
                                                AbstractOperator<INPUT, OUTPUT> jobCreator,
                                                AbstractSink<OUTPUT> sinkCreator) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!chaining) {
            executionEnvironment.disableOperatorChaining();
        }

        if (checkpointing) {
            executionEnvironment.enableCheckpointing(CHECKPOINTING_INTERVAL);
            executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(50);
        }

        DataStream<INPUT> source = sourceCreator.createSource(arguments, executionEnvironment);

        DataStream<OUTPUT> stream = jobCreator.createOperator(arguments, source);
        sinkCreator.createSink(arguments, stream);

        return executionEnvironment;
    }
}
