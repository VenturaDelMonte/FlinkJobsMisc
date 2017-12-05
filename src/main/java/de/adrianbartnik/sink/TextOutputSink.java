package de.adrianbartnik.sink;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TextOutputSink<T> implements FlinkJobFactory.SinkCreator<T> {

    private static final String OPERATOR_NAME = "JobSink";

    private static final String DEFAULT_PATH = "jobOutput";

    private final String path;

    public TextOutputSink() {
        this(DEFAULT_PATH);
    }

    public TextOutputSink(String path) {
        this.path = path;
    }

    @Override
    public void addSink(String[] arguments, DataStream<T> dataSource) {
        addSink(arguments, dataSource, 1);
    }

    @Override
    public void addSink(String[] arguments, DataStream<T> dataSource, int parallelism) {
        dataSource.writeAsText(path).name(OPERATOR_NAME).setParallelism(parallelism);
    }
}
