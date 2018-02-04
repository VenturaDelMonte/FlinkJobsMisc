package de.adrianbartnik.source;

import de.adrianbartnik.source.socketfunctions.AbstractSocketSourceFunction;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class GenericParallelSocketSource<T> extends AbstractSource<T> implements Serializable {

    private static final String OPERATOR_NAME = "ParallelSocketSource";

    private final AbstractSocketSourceFunction<T> function;

    public GenericParallelSocketSource(AbstractSocketSourceFunction<T> function, int parallelism) {
        super(parallelism);
        this.function = function;
    }

    @Override
    public DataStream<T> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {
        return new DataStreamSource(executionEnvironment,
                function.getTypeInfo(),
                new StreamSource(function),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }
}
