package de.adrianbartnik.source.socketfunctions;

import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class PersonSocketSourceFunction extends AbstractSocketSourceFunction<NewPersonEvent> {

    public PersonSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
        super(hostnames, ports);
    }

    @Override
    protected NewPersonEvent stringToRecord(String record) {
        String[] split = record.split(",");
        return new NewPersonEvent(
                Long.valueOf(split[0]),
                Integer.valueOf(split[1]),
                split[2],
                split[3],
                split[4],
                split[5],
                split[6],
                split[7],
                split[8],
                split[9]);
    }

    @Override
    public TypeInformation<NewPersonEvent> getTypeInfo() {
        return TypeInformation.of(new TypeHint<NewPersonEvent>() {});
    }
}
