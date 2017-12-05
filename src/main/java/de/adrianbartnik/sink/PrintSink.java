package de.adrianbartnik.sink;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PrintSink<T> implements FlinkJobFactory.SinkCreator {
    @Override
    public void addSink(String[] arguments, DataStream dataSource) {
        dataSource.print();
    }
}
