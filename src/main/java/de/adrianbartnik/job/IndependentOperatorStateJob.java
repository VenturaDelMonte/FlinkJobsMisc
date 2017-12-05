package de.adrianbartnik.job;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import de.adrianbartnik.source.StatefulIntervalSequenceSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class IndependentOperatorStateJob {

    private static final String JOB_NAME = "OperatorStateJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<Long, String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new StatefulIntervalSequenceSource(0, 100_000, 50),
                        new CountingMap<Long>(),
                        new TextOutputSink<String>());

        job.execute(JOB_NAME);
    }
}
