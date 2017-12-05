package de.adrianbartnik.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

/**
 * Each mapper counts how many items it has processed.
 */
public class CountingMap<INPUT> extends RichMapFunction<INPUT, String> implements ListCheckpointed<Long> {

    private long numberOfProcessedElements = 0;

    private String taskNameWithSubtasks;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
    }

    @Override
    public String map(INPUT value) throws Exception {
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
