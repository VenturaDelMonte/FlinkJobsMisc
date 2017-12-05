package de.adrianbartnik.job;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Simple job with one mapper, that appends text to all incoming tuples.
 */
public class AppendTextMapJob implements FlinkJobFactory.JobCreator<String> {

    private static final String JOB_NAME = "AppendTextMapJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new AppendTextMapJob(), new TextOutputSink());

        job.execute(JOB_NAME);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        return dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input + " - appended text";
            }
        }).name("AppendTextMapper");
    }
}
