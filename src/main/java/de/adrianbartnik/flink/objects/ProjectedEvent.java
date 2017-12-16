package de.adrianbartnik.flink.objects;

import java.sql.Timestamp;

public class ProjectedEvent {
    public final String ad_id;
    public final Timestamp event_time;

    public ProjectedEvent(String ad_id, Timestamp event_time) {
        this.ad_id = ad_id;
        this.event_time = event_time;
    }
}
