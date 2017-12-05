package de.adrianbartnik.sink;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PrintSink<T> implements FlinkJobFactory.SinkCreator {
    @Override
    public void addSink(String[] arguments, DataStream dataSource) {
        addSink(arguments, dataSource, 1);
    }

    @Override
    public void addSink(String[] arguments, DataStream dataSource, int parallelism) {
        dataSource.print();
    }
}
