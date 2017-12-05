package de.adrianbartnik.sink;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TextOutputSink<T> implements FlinkJobFactory.SinkCreator<T> {

    private static final String OPERATOR_NAME = "JobSink";

    private static final String DEFAULT_PATH = "jobOutput";

    private final int parallelism;
    private final String path;

    public TextOutputSink() {
        this(2, DEFAULT_PATH);
    }

    public TextOutputSink(int parallelism, String path) {
        this.parallelism = parallelism;
        this.path = path;
    }

    @Override
    public void addSink(String[] arguments, DataStream<T> dataSource) {
        dataSource.writeAsText(path).name(OPERATOR_NAME).setParallelism(parallelism);
    }
}
