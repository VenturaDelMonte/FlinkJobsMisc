package de.adrianbartnik;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a rabbitMQ instance and reads strings.
 */
public class LowerCaseMapJob extends AbstractRabbitMQMapJob {

    private static final String JOB_NAME = "LowerCaseMapJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        LowerCaseMapJob lowerCaseMapJob = new LowerCaseMapJob();

        lowerCaseMapJob.executeJob(args, JOB_NAME);
    }

    @Override
    protected void createJob(DataStream<String> source) {
        SingleOutputStreamOperator<String> LowerCaseMapper = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input.toLowerCase();
            }
        }).name("LowerCaseMapper");

        LowerCaseMapper.writeAsText("sinkTextOutput").name("JobSink").setParallelism(2);
    }
}
