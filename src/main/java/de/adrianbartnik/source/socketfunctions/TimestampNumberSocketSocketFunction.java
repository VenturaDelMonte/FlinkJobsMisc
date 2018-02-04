package de.adrianbartnik.source.socketfunctions;

import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;
import java.util.List;

public class TimestampNumberSocketSocketFunction extends AbstractSocketSourceFunction<Tuple2<Timestamp, String>> {

    public TimestampNumberSocketSocketFunction(List<String> hostnames, List<Integer> ports) {
        super(hostnames, ports);
    }

    @Override
    protected Tuple2<Timestamp, String> stringToRecord(String record) {
        if (record == null || !record.contains("#")) {
            throw new IllegalArgumentException("Malformed input from sockets: " + record);
        }

        String[] split = record.split("#");

        if (split.length != 2 || split[0].isEmpty() || split[1].isEmpty()) {
            throw new IllegalArgumentException("Malformed input from sockets: " + record);
        }

        return new Tuple2<>(new Timestamp(Long.valueOf(split[0])), split[1]);
    }

    @Override
    public TypeInformation<Tuple2<Timestamp, String>> getTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<Timestamp, String>>() {});
    }
}
