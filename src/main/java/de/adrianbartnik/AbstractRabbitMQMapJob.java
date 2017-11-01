package de.adrianbartnik;

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
public abstract class AbstractRabbitMQMapJob {

    private static final int CHECKPOINTING_INTERVAL = 500;

    void executeJob(String[] args, String jobName) throws Exception {
        executeJob(args, jobName, false);
    }

    void executeJob(String[] args, String jobName, boolean enableCheckpointing) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        final String queueName;
        final boolean chaining;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port") : 5672;
            queueName = params.has("queuename") ? params.get("queuename") : "defaultQueue";
            chaining = params.has("chaining") && params.getBoolean("chaining");
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

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!chaining) {
            env.disableOperatorChaining();
        }

        if (enableCheckpointing) {
            env.enableCheckpointing(CHECKPOINTING_INTERVAL);
        }

        final DataStream<String> stream = env
                .addSource(new RMQSource<>(
                        connectionConfig,
                        queueName,
                        true,       // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())) // deserialization schema to turn messages into Java objects
                .name("RabbitMQSource")
                .setParallelism(1);                // non-parallel source is only required for exactly-once

        createJob(stream);

        env.execute(jobName);
    }

    protected abstract void createJob(DataStream<String> source);
}
