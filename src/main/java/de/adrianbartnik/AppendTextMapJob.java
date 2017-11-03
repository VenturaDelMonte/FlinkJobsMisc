package de.adrianbartnik;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Simple job with one mapper, that appends text to all incoming tuples.
 */
public class AppendTextMapJob extends AbstractRabbitMQMapJob {

    private static final String JOB_NAME = "AppendTextMapJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        AppendTextMapJob appendTextMapJob = new AppendTextMapJob();

        appendTextMapJob.executeJob(args, JOB_NAME);
    }

    @Override
    protected void createJob(DataStream<String> source) {
        SingleOutputStreamOperator<String> LowerCaseMapper = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input + " - appended text";
            }
        }).name("AppendedTextMapper");

        LowerCaseMapper.writeAsText("sinkTextOutput").name("JobSink").setParallelism(2);
    }
}
