package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import de.adrianbartnik.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StatefulKafkaJob {

    private static final String JOB_NAME = "IndependentOperatorStateJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, true);

        StreamExecutionEnvironment job =
                creator.createJob(new KafkaSource(4),
                        new CountingMap<String>(6),
                        new TextOutputSink<String>(2));

        job.execute(JOB_NAME);
    }
}
