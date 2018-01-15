package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.ParallelSocketSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParallelSocketOperatorStateJob {

    private static final String JOB_NAME = "ParallelSocketOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");

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

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, true);

        StreamExecutionEnvironment job =
                creator.createJob(new ParallelSocketSource(hostnames, ports, sourceParallelism),
                        new CountingMap<String>(mapParallelism),
                        new TextOutputSink<String>(sinkParallelism));

        job.execute(JOB_NAME);
    }
}