package de.adrianbartnik.job.stateful;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingTupleMap;
import de.adrianbartnik.operator.JoiningNewUsersWithAuctionsFunction;
import de.adrianbartnik.sink.LatencySink;
import de.adrianbartnik.source.GenericParallelSocketSource;
import de.adrianbartnik.source.socketfunctions.AuctionSocketSourceFunction;
import de.adrianbartnik.source.socketfunctions.PersonSocketSourceFunction;
import de.adrianbartnik.source.socketfunctions.TimestampNumberSocketSocketFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.mortbay.log.Log;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NexmarkQuery8 {

    private static final String JOB_NAME = "ParallelSocketOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");
        final String output_path = params.get("path", "benchmarkOutput");

        if (hostnames_string == null || hostnames_string.isEmpty() || ports_string == null || ports_string.isEmpty()) {
            throw new IllegalArgumentException("Hostname and Ports must not be empty");
        }

        List<String> hostnames = Arrays.asList(hostnames_string.split(","));
        List<String> separated_ports = Arrays.asList(ports_string.split(","));

        List<Integer> ports = new ArrayList<>();
        for (String port : separated_ports) {
            ports.add(Integer.valueOf(port));
        }

        if (ports.size() != hostnames.size()) {
            throw new IllegalArgumentException("Hostname and Ports must be of equal size");
        }

        final int sourceParallelism = hostnames.size();
        for (int i = 0; i < hostnames.size(); i++) {
            Log.debug("Connecting to socket {}:{}", hostnames.get(i), ports.get(i));
        }

        StreamExecutionEnvironment streamExecutionEnvironment =
                new FlinkJobFactory(args, false, true).setupExecutionEnvironment();

        AuctionSocketSourceFunction auctionFunction = new AuctionSocketSourceFunction(hostnames, ports);
        GenericParallelSocketSource<AuctionEvent> auctionSource = new GenericParallelSocketSource<>(auctionFunction, sourceParallelism);

        PersonSocketSourceFunction personFunction = new PersonSocketSourceFunction(hostnames, ports);
        GenericParallelSocketSource<NewPersonEvent> personSource = new GenericParallelSocketSource<>(personFunction, sourceParallelism);

        personSource.createSource(args, streamExecutionEnvironment)
                .join(auctionSource.createSource(args, streamExecutionEnvironment))
                .where(NewPersonEvent::getPersonId).equalTo(AuctionEvent::getPersonId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new JoiningNewUsersWithAuctionsFunction())
                .print();

        streamExecutionEnvironment.execute(JOB_NAME);
    }
}
