package de.adrianbartnik.sink.latency;

import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.WindowedCount;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import de.adrianbartnik.sink.AbstractSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class YahooWindowCountLatencySink extends AbstractSink<WindowedCount> implements Serializable {

    private static final String OPERATOR_NAME = "WindowLatencySink";

    private static final long serialVersionUID = 1L;

    private final String path;

    public YahooWindowCountLatencySink(String path) {
        super();
        this.path = path;
    }

    public YahooWindowCountLatencySink(int parallelism, String path) {
        super(parallelism);
        this.path = path;
    }

    @Override
    public void createSink(String[] arguments, DataStream<WindowedCount> dataSource) {
        dataSource
                .writeUsingOutputFormat(new WindowLatencyOutputFormat(new Path(path)))
                .setParallelism(parallelism)
                .name(OPERATOR_NAME);
    }

    private class WindowLatencyOutputFormat extends AbstractOutputFormat<WindowedCount> {

        WindowLatencyOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        StringBuilder getOutputString(WindowedCount record) {

            this.stringBuilder.append(record.campaignId);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.count);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.lastUpdate.getTime());
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.timeWindow.getTime());

            return this.stringBuilder;
        }
    }

}
