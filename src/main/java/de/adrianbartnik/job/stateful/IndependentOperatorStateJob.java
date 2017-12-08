package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IndependentOperatorStateJob {

    private static final String JOB_NAME = "IndependentOperatorStateJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<Long, String> creator = new FlinkJobFactory<>(args, false, true);

        StreamExecutionEnvironment job =
                creator.createJob(new IntervalSequenceSource(0, 100_000, 50),
                        new CountingMap<Long>(),
                        new TextOutputSink<String>());

        job.execute(JOB_NAME);
    }
}
