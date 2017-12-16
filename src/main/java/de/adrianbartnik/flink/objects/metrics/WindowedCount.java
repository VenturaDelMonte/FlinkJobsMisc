package de.adrianbartnik.flink.objects.metrics;

import java.sql.Timestamp;


public class WindowedCount {
    public final Timestamp time_window;
    public final String campaign_id;
    public Long count;
    public Timestamp lastUpdate;

    /**
     * Class used to aggregate windowed counts.
     *
     * @param lastUpdate Event time of the last record received for tmp given `campaign_id` and `time_window`
     */
    public WindowedCount(Timestamp time_window, String campaign_id, long count, Timestamp lastUpdate) {
        this.time_window = time_window;
        this.campaign_id = campaign_id;
        this.count = count;
        this.lastUpdate = lastUpdate;
    }
}
