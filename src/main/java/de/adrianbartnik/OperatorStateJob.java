package de.adrianbartnik;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Collections;
import java.util.List;

public class OperatorStateJob extends AbstractRabbitMQMapJob {
    @Override
    protected void createJob(DataStream<String> source) {
        SingleOutputStreamOperator<String> countingMap =
                source.map(new CountingMap()).name("CountingMap").setParallelism(3);

        countingMap.writeAsText("operatorStateJobSink").name("OperatorStateJob").setParallelism(2);
    }

    /**
     * Each mapper counts how many items it has processed.
     */
    private class CountingMap extends RichMapFunction<String, String> implements ListCheckpointed<Long> {

        private transient Long numberOfProcessedElements;

        private transient String taskNameWithSubtasks;

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
