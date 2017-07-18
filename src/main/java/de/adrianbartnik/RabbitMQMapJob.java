package de.adrianbartnik;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a rabbitMQ instance and reads strings.
 */
public class RabbitMQMapJob {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        final String queueName;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port") : 5672;
            queueName = params.has("queuename") ? params.get("queuename") : "defaultQueue";
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'RabbitMQMapJob " +
                    "--hostname <hostname> --port <port>' --queueName <queueName>, where hostname " +
                    "(localhost by default) and port is the address of the rabbitMQ instance");
            return;
        }

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(hostname)
                .setPort(port)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,
                        queueName,
                        true,       // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())) // deserialization schema to turn messages into Java objects
                .setParallelism(1);                // non-parallel source is only required for exactly-once

        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toLowerCase();
            }
        }).name("CustomMap").print().name("PrintSink").setParallelism(1);

        env.execute("RabbitMQMapJob for Masterthesis");
    }
}
