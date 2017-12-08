package de.adrianbartnik.sink;

import org.apache.flink.streaming.api.datastream.DataStream;

public class TextOutputSink<T> extends AbstractSink<T> {

    private static final String OPERATOR_NAME = "JobSink";

    private static final String DEFAULT_PATH = "jobOutput";

    private final String path;

    public TextOutputSink() {
        this(DEFAULT_PATH);
    }

    public TextOutputSink(int parallelism) {
        super(parallelism);
        path = DEFAULT_PATH;
    }

    public TextOutputSink(String path) {
        this.path = path;
    }

    public TextOutputSink(int parallelism, String path) {
        super(parallelism);
        this.path = path;
    }

    @Override
    public void createSink(String[] arguments, DataStream<T> dataSource) {
        dataSource.writeAsText(path).name(OPERATOR_NAME).setParallelism(parallelism);
    }
}
