package de.adrianbartnik.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ParallelSocketSource extends AbstractSource<String> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelSocketSource.class);

    private static final String OPERATOR_NAME = "ParallelSocketSource";

    private final List<String> hostnames;
    private final List<Integer> ports;

    public ParallelSocketSource(List<String> hostnames, List<Integer> ports, int parallelism) {
        super(parallelism);

        checkNotNull(hostnames, "Hostnames must not be null");
        checkArgument(hostnames.size() == parallelism, "Number of hostnames does not match degree of parallelism");

        this.hostnames = hostnames;
        this.ports = ports;
    }

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {
        return new DataStreamSource<>(executionEnvironment,
                TypeInformation.of(String.class),
                new StreamSource<>(new SocketTextStreamFunction(hostnames, ports)),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    @PublicEvolving
    public class SocketTextStreamFunction extends RichParallelSourceFunction<String> {

        private static final long serialVersionUID = 1L;

        /** Default delay between successive connection attempts. */
        private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

        /** Default connection timeout when connecting to the server socket (infinite). */
        private static final int CONNECTION_TIMEOUT_TIME = 0;

        private final List<String> hostnames;
        private final List<Integer> ports;
        private final String delimiter = "\n";
        private final long maxNumRetries;
        private final long delayBetweenRetries;

        private transient Socket currentSocket;

        private volatile boolean isRunning = true;

        public SocketTextStreamFunction(List<String> hostnames, List<Integer> ports) {
            this.hostnames = checkNotNull(hostnames, "Hostnames must not be null");
            this.ports = checkNotNull(ports, "Ports must not be null");
            this.maxNumRetries = 5;
            this.delayBetweenRetries = DEFAULT_CONNECTION_RETRY_SLEEP;

            for (Integer port : ports) {
                checkArgument(port > 0 && port < 65536, "ports is out of range");
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();
            long attempt = 0;

            checkArgument(hostnames.size() == getRuntimeContext().getNumberOfParallelSubtasks(),
                    "Number of hostnames does not match degree of parallelism");

            while (isRunning) {

                try (Socket socket = new Socket()) {
                    currentSocket = socket;

                    String hostname = hostnames.get(getRuntimeContext().getIndexOfThisSubtask());
                    int port = ports.get(getRuntimeContext().getIndexOfThisSubtask());

                    LOG.info("Connecting to server socket " + hostname + ':' + port);
                    socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    char[] cbuf = new char[8192];
                    int bytesRead;
                    while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                        buffer.append(cbuf, 0, bytesRead);
                        int delimPos;
                        while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
                            String record = buffer.substring(0, delimPos);
                            // truncate trailing carriage return
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            ctx.collect(record);
                            buffer.delete(0, delimPos + delimiter.length());
                        }
                    }
                }

                // if we dropped out of this loop due to an EOF, sleep and retry
                if (isRunning) {
                    attempt++;
                    if (maxNumRetries == -1 || attempt < maxNumRetries) {
                        LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
                        Thread.sleep(delayBetweenRetries);
                    }
                    else {
                        // this should probably be here, but some examples expect simple exists of the stream source
                        // throw new EOFException("Reached end of stream and reconnects are not enabled.");
                        break;
                    }
                }
            }

            // collect trailing data
            if (buffer.length() > 0) {
                ctx.collect(buffer.toString());
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

            // we need to close the socket as well, because the Thread.interrupt() function will
            // not wake the thread in the socketStream.read() method when blocked.
            Socket theSocket = this.currentSocket;
            if (theSocket != null) {
                IOUtils.closeSocket(theSocket);
            }
        }
    }
}
