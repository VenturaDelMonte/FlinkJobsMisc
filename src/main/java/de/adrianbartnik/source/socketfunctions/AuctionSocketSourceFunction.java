package de.adrianbartnik.source.socketfunctions;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class AuctionSocketSourceFunction extends AbstractSocketSourceFunction<AuctionEvent> {

    public AuctionSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
        super(hostnames, ports);
    }

    @Override
    protected AuctionEvent stringToRecord(String record) {
        return new AuctionEvent();
    }

    @Override
    public TypeInformation<AuctionEvent> getTypeInfo() {
        return TypeInformation.of(new TypeHint<AuctionEvent>() {});
    }
}
