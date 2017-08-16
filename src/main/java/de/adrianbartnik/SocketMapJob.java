package de.adrianbartnik;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class SocketMapJob {

    private static transient final Logger LOG = LoggerFactory.getLogger(SocketMapJob.class);

    private static final String JOB_NAME = "SocketMapJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        final boolean chaining;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port") : 9000;
            chaining = params.has("chaining") && params.getBoolean("chaining");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketMapJob " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (!chaining) {
            env.disableOperatorChaining();
        }

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        text.map(new MapFunction<String, String>() {
            @Override
            public String map(String tuple) throws Exception {
                return tuple.toLowerCase();
            }
        }).name("CustomMap").print().name("PrintSink").setParallelism(1);

        env.execute(JOB_NAME);
    }
}
