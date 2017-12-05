package de.adrianbartnik.operator;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TextAppender implements FlinkJobFactory.JobCreator<String, String>{

    private static final String OPERATOR_NAME = "TextAppender";

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        return addOperators(arguments, dataSource, 4);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource, int parallelism) {
        return dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input + " - appended text";
            }
        }).name(OPERATOR_NAME).setParallelism(parallelism);
    }
}