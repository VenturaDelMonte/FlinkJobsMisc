package de.adrianbartnik.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.*;
import java.sql.Timestamp;

public class LatencySink extends AbstractSink<Tuple4<Timestamp, String, String, Long>> implements Serializable {

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
    public void createSink(String[] arguments, DataStream<Tuple4<Timestamp, String, String, Long>> dataSource) {
        dataSource
                .writeUsingOutputFormat(new CustomLatencyOutputFormat<>(new Path(path)))
                .setParallelism(parallelism);
    }

    /**
     * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The output is
     * structured by record delimiters and field delimiters as common in CSV files.
     * Record delimiter separate records from each other ('\n' is common). Field
     * delimiters separate fields within a record.
     */
    public class CustomLatencyOutputFormat<T extends Tuple4<Timestamp, String, String, Long>> extends FileOutputFormat<T>
            implements InputTypeConfigurable {

        private static final long serialVersionUID = 1L;

        private transient Writer wrt;

        private final String fieldDelimiter = CsvInputFormat.DEFAULT_FIELD_DELIMITER;

        private final String recordDelimiter = CsvInputFormat.DEFAULT_LINE_DELIMITER;

        private final String charsetName = "UTF-8";

        /**
         * Creates an instance of CustomLatencyOutputFormat.
         *
         * @param outputPath The path where the file is written.
         */
        public CustomLatencyOutputFormat(Path outputPath) {
            super(outputPath);
        }

        // --------------------------------------------------------------------------------------------

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            super.open(taskNumber, numTasks);
            this.wrt = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
        }

        @Override
        public void close() throws IOException {
            if (wrt != null) {
                this.wrt.flush();
                this.wrt.close();
            }
            super.close();
        }

        @Override
        public void writeRecord(T element) throws IOException {

            this.wrt.write("" + (System.currentTimeMillis() - element.f0.getTime()));
            this.wrt.write(this.fieldDelimiter);
            this.wrt.write(element.f1);
            this.wrt.write(this.fieldDelimiter);
            this.wrt.write(element.f2);
            this.wrt.write(this.fieldDelimiter);
            this.wrt.write("" + element.f3);

            this.wrt.write(this.recordDelimiter);
            this.wrt.flush();
        }

        // --------------------------------------------------------------------------------------------
        @Override
        public String toString() {
            return "CustomLatencyOutputFormat (path: " + this.getOutputFilePath() + ", delimiter: " + this.fieldDelimiter + ")";
        }

        /**
         * The purpose of this method is solely to check whether the data type to be processed
         * is in fact a tuple type.
         */
        @Override
        public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
            if (!type.isTupleType()) {
                throw new InvalidProgramException("The " + CustomLatencyOutputFormat.class.getSimpleName() +
                        " can only be used to write tuple data sets.");
            }
        }
    }

}
