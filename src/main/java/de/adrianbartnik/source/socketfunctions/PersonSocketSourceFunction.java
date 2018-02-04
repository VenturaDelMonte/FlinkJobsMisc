package de.adrianbartnik.source.socketfunctions;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
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
        return new NewPersonEvent();
    }

    @Override
    public TypeInformation<NewPersonEvent> getTypeInfo() {
        return TypeInformation.of(new TypeHint<NewPersonEvent>() {});
    }
}
