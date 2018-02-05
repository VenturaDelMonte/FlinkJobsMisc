package de.adrianbartnik.job.timestampextractor;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class AuctionEventTimestampExtractor extends AscendingTimestampExtractor<AuctionEvent> {
    @Override
    public long extractAscendingTimestamp(AuctionEvent element) {
        return element.getTimestamp();
    }
}
