package de.adrianbartnik.job;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a rabbitMQ instance and reads strings.
 */
public class ChangeOfDoPJob implements FlinkJobFactory.JobCreator<String> {

    private static final String JOB_NAME = "ChangeOfDoPJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new ChangeOfDoPJob(), new TextOutputSink());

        job.execute(JOB_NAME);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        SingleOutputStreamOperator<String> lowerCaseMapper = dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return "Received: " + input;
            }
        }).setParallelism(3).name("ReceivedMapper");

        return lowerCaseMapper.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String string) throws Exception {
                return Long.valueOf(string.substring(10)) % 2 == 0;
            }
        }).setParallelism(2).name("Modulo2Filter");
    }
}
