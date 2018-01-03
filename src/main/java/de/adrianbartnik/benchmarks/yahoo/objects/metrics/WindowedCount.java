package de.adrianbartnik.benchmarks.yahoo.objects.metrics;

import java.io.Serializable;
import java.sql.Timestamp;


public class WindowedCount implements Serializable {
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

    @Override
    public String toString() {
        return "WindowedCount - time_window: " + time_window + " campaign_id: " + campaign_id
                + " count: " + count + " last_update: " + lastUpdate;
    }
}
