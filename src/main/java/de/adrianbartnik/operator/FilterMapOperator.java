package de.adrianbartnik.operator;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class FilterMapOperator implements FlinkJobFactory.JobCreator<String, String> {

    private static final String MAP_OPERATOR_NAME = "AppendMapper";
    private static final String FILTER_OPERATOR_NAME = "AppendMapper";

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        return addOperators(arguments, dataSource, 4);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource, int parallelism) {

        SingleOutputStreamOperator<String> filterOperator = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String string) throws Exception {
                return Long.valueOf(string.substring(10)) % 2 == 0;
            }
        }).setParallelism(parallelism).name(MAP_OPERATOR_NAME);

        return filterOperator.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return "Received: " + input;
            }
        }).setParallelism(3).name(FILTER_OPERATOR_NAME);
    }
}