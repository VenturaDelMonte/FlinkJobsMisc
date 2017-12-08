package de.adrianbartnik.operator;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class LowercaseMapper extends AbstractOperator<String, String> {

    private static final String OPERATOR_NAME = "LowercaseMap";

    public LowercaseMapper() {
        super();
    }

    public LowercaseMapper(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<String> createOperator(String[] arguments, DataStream<String> dataSource) {
        return dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String tuple) throws Exception {
                return tuple.toLowerCase();
            }
        }).setParallelism(parallelism).name(OPERATOR_NAME);
    }
}
