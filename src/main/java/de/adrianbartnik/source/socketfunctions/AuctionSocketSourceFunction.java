package de.adrianbartnik.source.socketfunctions;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

/**
 * Schema: timestamp,auction_id,item_id,seller_id,category_id,quantity,type,start,end
 */
public class AuctionSocketSourceFunction extends AbstractSocketSourceFunction<AuctionEvent> {

    public AuctionSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
        super(hostnames, ports);
    }

    @Override
    protected AuctionEvent stringToRecord(String record) {
        String[] split = record.split(",");
        return new AuctionEvent(
                Long.valueOf(split[0]),
                Integer.valueOf(split[1]),
                Integer.valueOf(split[2]),
                Integer.valueOf(split[3]),
                Double.valueOf(split[4]),
                Long.valueOf(split[5]),
                Long.valueOf(split[6]),
                Long.valueOf(split[7]));
    }

    @Override
    public TypeInformation<AuctionEvent> getTypeInfo() {
        return TypeInformation.of(new TypeHint<AuctionEvent>() {});
    }

    @Override
    protected String getStartCommand() {
        return "auctions\n";
    }
}
