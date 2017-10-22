package de.adrianbartnik;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a rabbitMQ instance and reads strings.
 */
public class ChangeOfDoPJob extends AbstractRabbitMQMapJob {

    private static final String JOB_NAME = "ChangeOfDoPJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        ChangeOfDoPJob changeOfDoPJob = new ChangeOfDoPJob();

        changeOfDoPJob.executeJob(args, JOB_NAME);
    }

    @Override
    protected void createJob(DataStream<String> source) {

        SingleOutputStreamOperator<String> lowerCaseMapper = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return "Received: " + input;
            }
        }).setParallelism(3).name("ReceivedMapper");

        SingleOutputStreamOperator<String> module2filter = lowerCaseMapper.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String string) throws Exception {
                return Long.valueOf(string.substring(10)) % 2 == 0;
            }
        }).setParallelism(2).name("Modulo2Filter");

        module2filter.writeAsText("sinkTextOutput").name("JobSink").setParallelism(2);
    }
}
