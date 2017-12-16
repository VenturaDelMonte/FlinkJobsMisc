package de.adrianbartnik.flink.objects;

import java.sql.Timestamp;

/**
 * Input data
 */
public class Event {

    public static final String CAMPAIGNS_TOPIC = "campaigns";
    public static final String EVENTS_TOPIC = "events";
    public static final String OUTPUT_TOPIC = "output";

    public final String user_id;
    public final String page_id;
    public final String ad_id;
    public final String ad_type;
    public final String event_type;
    public final Timestamp event_time;
    public final String ip_address;

    public Event(String user_id, String page_id, String ad_id, String ad_type, String event_type, Timestamp event_time, String ip_address){
        this.user_id = user_id;
        this.page_id = page_id;
        this.ad_id = ad_id;
        this.ad_type = ad_type;
        this.event_type = event_type;
        this.event_time = event_time;
        this.ip_address = ip_address;
    }
}
