package de.adrianbartnik.benchmarks.yahoo.operators;

import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.JoinedEventWithCampaign;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * Joins event id with advertising id and the event timestamp
 */
public class StaticJoinMapper implements MapFunction<Event, JoinedEventWithCampaign> {

    private final Map<String, String> campaigns;

    public StaticJoinMapper(Map<String, String> campaigns) {
        this.campaigns = campaigns;
    }

    @Override
    public JoinedEventWithCampaign map(Event value) {
        return new JoinedEventWithCampaign(campaigns.get(value.advertisingId), value.advertisingId, value.eventTime);
    }
}
