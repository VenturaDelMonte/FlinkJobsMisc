package de.adrianbartnik.source;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class RabbitMQSource implements FlinkJobFactory.SourceCreator<String> {
    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        final String hostname;
        final int port;
        final String queueName;

        try {
            final ParameterTool params = ParameterTool.fromArgs(arguments);
            hostname = params.get("hostname", "localhost");
            port = params.getInt("port", 5672);
            queueName = params.get("queuename", "defaultQueue");
        } catch (Exception e) {
            System.err.println("No port specified. Please run '<jar> " +
                    "--hostname <hostname> --port <port>' --queueName <queueName>, where hostname " +
                    "(localhost by default) and port is the address of the rabbitMQ instance");
            throw new RuntimeException("No port specified.");
        }

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(hostname)
                .setPort(port)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();

        return executionEnvironment
                .addSource(new RMQSource<>(
                        connectionConfig,
                        queueName,
                        true,       // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())) // deserialization schema to turn messages into Java objects
                .name("RabbitMQSource")
                .setParallelism(1);                // non-parallel source is only required for exactly-once
    }
}