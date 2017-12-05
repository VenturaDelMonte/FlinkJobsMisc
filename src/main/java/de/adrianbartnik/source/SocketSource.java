package de.adrianbartnik.source;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource implements FlinkJobFactory.SourceCreator<String> {

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {
        return createSource(arguments, executionEnvironment, 1);
    }

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment, int parallelism) {
        // the host and the port to connect to
        final ParameterTool params = ParameterTool.fromArgs(arguments);
        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 9000);
        final String delimiter = params.get("delimiter", "\n");

        return executionEnvironment.socketTextStream(hostname, port, delimiter);
    }
}
