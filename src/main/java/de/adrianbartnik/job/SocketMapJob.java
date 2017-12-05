package de.adrianbartnik.job;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
public class SocketMapJob implements FlinkJobFactory.JobCreator<String, String> {

    private static final String JOB_NAME = "SocketMapJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new SocketMapJob(), new TextOutputSink<String>());

        job.execute(JOB_NAME);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        return dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String tuple) throws Exception {
                return tuple.toLowerCase();
            }
        }).name("LowerCaseMapper");
    }
}