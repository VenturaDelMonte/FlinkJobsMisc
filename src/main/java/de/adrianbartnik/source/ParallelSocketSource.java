package de.adrianbartnik.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.MalformedInputException;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ParallelSocketSource extends AbstractSource<Tuple2<Timestamp, String>> implements Serializable {

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
    public DataStream<Tuple2<Timestamp, String>> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        TypeInformation<Tuple2<Timestamp, String>> info = TypeInformation.of(new TypeHint<Tuple2<Timestamp, String>>() {});

        return new DataStreamSource<>(executionEnvironment,
                info,
                new StreamSource<>(new SocketTextStreamFunction(hostnames, ports)),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    @PublicEvolving
    public class SocketTextStreamFunction extends RichParallelSourceFunction<Tuple2<Timestamp, String>>
            implements CheckpointedFunction, StoppableFunction {

        private static final String STATE_HOSTNAMES = "hostnames";
        private static final String STATE_PORTS = "ports";
        private static final String STATE_RECORDS = "records";

        private static final long serialVersionUID = 1L;

        /**
         * Default connection timeout when connecting to the server socket (infinite).
         */
        private static final int CONNECTION_TIMEOUT_TIME = 0;


        private final List<String> hostnames;
        private final List<Integer> ports;
        private final String delimiter = "\n";

        private boolean restored;
        private String hostname;
        private int port;
        private long numberProcessedMessages;

        private ListState<String> listStateHostnames;
        private ListState<Integer> listStatePorts;
        private ListState<Long> listStateNumberOfProcessedRecords;

        private transient Socket currentSocket;

        private volatile boolean isRunning = true;

        public SocketTextStreamFunction(List<String> hostnames, List<Integer> ports) {
            this.hostnames = checkNotNull(hostnames, "Hostnames must not be null");
            this.ports = checkNotNull(ports, "Ports must not be null");

            for (Integer port : ports) {
                checkArgument(port > 0 && port < 65536, "ports is out of range");
            }
        }

        @Override
        public void run(SourceContext<Tuple2<Timestamp, String>> ctx) throws Exception {
            final StringBuilder buffer = new StringBuilder();

            checkArgument(hostnames.size() == getRuntimeContext().getNumberOfParallelSubtasks(),
                    "Number of hostnames does not match degree of parallelism");

            try (Socket socket = new Socket()) {
                currentSocket = socket;

                hostname = chooseHostname();
                port = choosePort();
                numberProcessedMessages = restoreProcessedMessages();

                LOG.info("Connecting to server socket {}:{} with current number of messages {}",
                        hostname, port, numberProcessedMessages);

                socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);

                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintStream output = new PrintStream(socket.getOutputStream(), true);

                // Necessary, so port stays open after disconnect
                output.print("from:" + numberProcessedMessages + ":reconnect\n");

                char[] cbuf = new char[8192];
                int bytesRead;
                while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                    buffer.append(cbuf, 0, bytesRead);
                    int delimPos;
                    while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
                        String record = buffer.substring(0, delimPos);
                        // truncate trailing carriage return
                        if (record.endsWith("\r")) {
                            record = record.substring(0, record.length() - 1);
                        }

                        synchronized (ctx.getCheckpointLock()) {

                            if (!isRunning) {
                                return;
                            }

                            Tuple2<Timestamp, String> tuple = objectToRecord(record);
                            ctx.collect(tuple);
                            numberProcessedMessages++;
                        }

                        buffer.delete(0, delimPos + delimiter.length());
                    }
                }
            }
        }

        private Tuple2<Timestamp, String> objectToRecord(String object) {
            if (object == null || !object.contains("#")) {
                throw new IllegalArgumentException("Malformed input from sockets: " + object);
            }

            String[] split = object.split("#");

            if (split.length != 2 || split[0].isEmpty() || split[1].isEmpty()) {
                throw new IllegalArgumentException("Malformed input from sockets: " + object);
            }

            return new Tuple2<>(new Timestamp(Long.valueOf(split[0])), split[1]);
        }

        private String chooseHostname() throws Exception {
            String localHostname = hostnames.get(getRuntimeContext().getIndexOfThisSubtask()), remoteHostname = "";

            for (String hostname : listStateHostnames.get()) {
                remoteHostname = hostname;
            }

            if (restored && !localHostname.equals(remoteHostname)) {
                throw new IllegalStateException("Restored hostname differs from originally assigned");
            }

            return localHostname;
        }

        private int choosePort() throws Exception {
            int localPort = ports.get(getRuntimeContext().getIndexOfThisSubtask()), restoredPort = -1;

            for (Integer port : listStatePorts.get()) {
                restoredPort = port;
            }

            if (restored && localPort != restoredPort) {
                throw new IllegalStateException("Restored port differs from originally assigned");
            }

            return localPort;
        }

        private long restoreProcessedMessages() throws Exception {
            if (!restored) {
                return 0;
            } else {

                long messages = -1;

                for (Long numberOfMessages : listStateNumberOfProcessedRecords.get()) {
                    if (messages == -1) {
                        messages = numberOfMessages;
                    } else {
                        throw new IllegalStateException("Multiple number of processed records");
                    }
                }

                if (messages == -1) {
                    throw new IllegalStateException("Not old number of messages could be restored");
                }

                return messages;
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

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            LOG.debug("Taking checkpoint of source {} of host {} on port {} with messages {} and is running? {}",
                    getRuntimeContext().getIndexOfThisSubtask(), hostname, port, numberProcessedMessages, isRunning);

            listStateHostnames.clear();
            listStatePorts.clear();
            listStateNumberOfProcessedRecords.clear();

            listStateHostnames.add(hostname);
            listStatePorts.add(port);
            listStateNumberOfProcessedRecords.add(numberProcessedMessages);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            OperatorStateStore stateStore = context.getOperatorStateStore();

            listStateHostnames = stateStore.getListState(new ListStateDescriptor<>(STATE_HOSTNAMES, String.class));
            listStatePorts = stateStore.getListState(new ListStateDescriptor<>(STATE_PORTS, Integer.class));
            listStateNumberOfProcessedRecords = stateStore.getListState(new ListStateDescriptor<>(STATE_RECORDS, Long.class));

            if (context.isRestored()) {

                Preconditions.checkArgument(Iterables.size(listStateHostnames.get()) == 1,
                        "More than one hostname received");
                Preconditions.checkArgument(Iterables.size(listStatePorts.get()) == 1,
                        "More than one port received");
                Preconditions.checkArgument(Iterables.size(listStateNumberOfProcessedRecords.get()) == 1,
                        "More than one recorded message state received");

                restored = true;
            } else {
                LOG.info("No restore state for ParallelSocketSource");
            }
        }

        @Override
        public void stop() {
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
