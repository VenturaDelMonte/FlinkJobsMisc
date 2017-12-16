package de.adrianbartnik.factory;

import de.adrianbartnik.operator.AbstractOperator;
import de.adrianbartnik.sink.AbstractSink;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkJobFactory<INPUT, OUTPUT> {

    private static final int CHECKPOINTING_INTERVAL = 1000;

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
            executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        }

        DataStream<INPUT> source = sourceCreator.createSource(arguments, executionEnvironment);

        source.keyBy("one")
                .keyBy("df")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<INPUT>() {
                    @Override
                    public INPUT reduce(INPUT value1, INPUT value2) throws Exception {
                        return value1;
                    }
                });

        DataStream<OUTPUT> stream = jobCreator.createOperator(arguments, source);
        sinkCreator.createSink(arguments, stream);

        return executionEnvironment;
    }
}
