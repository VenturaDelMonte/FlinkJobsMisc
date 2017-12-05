package de.adrianbartnik.job;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class OperatorStateJob implements FlinkJobFactory.JobCreator<String> {

    private static final String JOB_NAME = "OperatorStateJob for Masterthesis";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new OperatorStateJob(), new TextOutputSink());

        job.execute(JOB_NAME);
    }

    @Override
    public DataStream<String> addOperators(String[] arguments, DataStream<String> dataSource) {
        return dataSource.map(new CountingMap()).name("CountingMap");
    }

    /**
     * Each mapper counts how many items it has processed.
     */
    private class CountingMap extends RichMapFunction<String, String> implements ListCheckpointed<Long> {

        private long numberOfProcessedElements = 0;

        private String taskNameWithSubtasks;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
        }

        @Override
        public String map(String value) throws Exception {
            numberOfProcessedElements++;
            return value + " - " + taskNameWithSubtasks + " - " + numberOfProcessedElements;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(numberOfProcessedElements);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            for (Long number : state) {
                numberOfProcessedElements += number;
            }
        }
    }
}
