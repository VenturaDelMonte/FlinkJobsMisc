package de.adrianbartnik.sink.latency;

import de.adrianbartnik.sink.AbstractSink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;

public class LatencySink extends AbstractSink<Tuple4<Timestamp, Long, String, Long>> implements Serializable {

    private static final String OPERATOR_NAME = "LatencySink";

    private static final long serialVersionUID = 1L;

    private final String path;

    public LatencySink(String path) {
        super();
        this.path = path;
    }

    public LatencySink(int parallelism, String path) {
        super(parallelism);
        this.path = path;
    }

    @Override
    public void createSink(String[] arguments, DataStream<Tuple4<Timestamp, Long, String, Long>> dataSource) {
        dataSource
                .writeUsingOutputFormat(new CustomLatencyOutputFormat(new Path(path)))
                .setParallelism(parallelism)
                .name(OPERATOR_NAME);
    }

    /**
     * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The output is
     * structured by record delimiters and field delimiters as common in CSV files.
     * Record delimiter separate records from each other ('\n' is common). Field
     * delimiters separate fields within a record.
     */
    private class CustomLatencyOutputFormat extends AbstractOutputFormat<Tuple4<Timestamp, Long, String, Long>> {

        CustomLatencyOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        StringBuilder getOutputString(Tuple4<Timestamp, Long, String, Long> record) {

            this.stringBuilder.append((System.currentTimeMillis() - record.f0.getTime()));
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.f1);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.f2);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(record.f3);

            return this.stringBuilder;
        }
    }

}
