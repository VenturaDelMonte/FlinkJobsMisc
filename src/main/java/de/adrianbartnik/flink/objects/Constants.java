package de.adrianbartnik.flink.objects;

import java.util.Arrays;
import java.util.List;

/**
 * Static variables used through out the benchmark.
 */
public class Constants {

    public static final String CAMPAIGNS_TOPIC = "campaigns";
    public static final String EVENTS_TOPIC = "events";
    public static final String OUTPUT_TOPIC = "output";

    public static final List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    public static final List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");

    private Constants() {
    }
}
