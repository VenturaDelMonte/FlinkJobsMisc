package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IndependentOperatorStateJob {

    private static final String JOB_NAME = "IndependentOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int maxNumberOfMessages = params.getInt("maxNumberOfMessages", 100_000);
        final int pauseDuration = params.getInt("pauseDuration", 50);
        final int sourceParallelism = params.getInt("sourceParallelism", 3);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);

        FlinkJobFactory<Long, String> creator = new FlinkJobFactory<>(args, false, true);

        StreamExecutionEnvironment job =
                creator.createJob(new IntervalSequenceSource(0, maxNumberOfMessages, pauseDuration, sourceParallelism),
                        new CountingMap<Long>(mapParallelism),
                        new TextOutputSink<String>(sinkParallelism));

        job.execute(JOB_NAME);
    }
}
