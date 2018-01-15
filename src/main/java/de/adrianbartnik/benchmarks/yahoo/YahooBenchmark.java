package de.adrianbartnik.benchmarks.yahoo;

import de.adrianbartnik.benchmarks.yahoo.objects.CampaignAd;
import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import de.adrianbartnik.benchmarks.yahoo.objects.metrics.WindowedCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.util.*;

public class YahooBenchmark {
    // Transcribed from https://github.com/dataArtisans/yahoo-streaming-benchmark/blob/d8381f473ab0b72e33469d2b98ed1b77317fe96d/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyFlinkWindows.java#L179
    static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        private final int triggerIntervalMs;
        private long nextTimer = 0L;

        EventAndProcessingTimeTrigger(int triggerIntervalMs) {
            this.triggerIntervalMs = triggerIntervalMs;
        }

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // register system timer only for the first time
            ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, Boolean.FALSE);
            if (!firstTimerSet.value()) {
                nextTimer = System.currentTimeMillis() + triggerIntervalMs;
                ctx.registerProcessingTimeTimer(nextTimer);
                firstTimerSet.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // schedule next timer
            nextTimer = System.currentTimeMillis() + triggerIntervalMs;
            ctx.registerProcessingTimeTimer(nextTimer);
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteProcessingTimeTimer(nextTimer);
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }

    /**
     * A logger that prints out the number of records processed and the timestamp, which we can later use for throughput calculation.
     */
    static class ThroughputLogger implements FlatMapFunction<Event, Event> {

        private final long logFreq;

        private long lastTotalReceived = 0L;
        private long lastTime = 0L;
        private long totalReceived = 0L;

        ThroughputLogger(long logFreq) {
            this.logFreq = logFreq;
        }

        @Override
        public void flatMap(Event value, Collector<Event> out) throws Exception {
            if (totalReceived == 0) {
                System.out.println("ThroughputLogging: " + System.currentTimeMillis() + "," + totalReceived);
            }
            totalReceived += 1;
            if (totalReceived % logFreq == 0) {
                long currentTime = System.currentTimeMillis();
                System.out.println("Throughput: " + (totalReceived - lastTotalReceived) / (currentTime - lastTime) * 1000.0d);
                lastTime = currentTime;
                lastTotalReceived = totalReceived;
                System.out.println("ThroughputLogging: " + System.currentTimeMillis() + "," + totalReceived);
            }
            out.collect(value);
        }
    }

    static class StaticJoinMapper implements FlatMapFunction<Event, Tuple3<String, String, Timestamp>> {

        private final Map<String, String> campaigns;

        public StaticJoinMapper(Map<String, String> campaigns) {
            this.campaigns = campaigns;
        }

        @Override
        public void flatMap(Event value, Collector<Tuple3<String, String, Timestamp>> out) throws Exception {
            out.collect(new Tuple3<>(campaigns.get(value.ad_id), value.ad_id, value.event_time));
        }
    }

    static class AdTimestampExtractor implements TimestampExtractor<Tuple3<String, String, Timestamp>> {

        long maxTimestampSeen = 0L;

        @Override
        public long extractTimestamp(Tuple3<String, String, Timestamp> element, long currentTimestamp) {
            long timestamp = element.f2.getTime();
            maxTimestampSeen = Math.max(timestamp, maxTimestampSeen);
            return timestamp;
        }

        @Override
        public long extractWatermark(Tuple3<String, String, Timestamp> element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return maxTimestampSeen - 1L;
        }
    }

    public static void main(String args[]) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        Time windowMillis = Time.milliseconds(params.getLong("windowMillis", 10000));
        int parallelism = params.getInt("parallelism", 5);
        Preconditions.checkArgument(parallelism > 0, "Parallelism needs to be tmp positive integer.");
        // Used for assigning event times from out of order data

        // Used when generating input
        int numCampaigns = params.getInt("numCampaigns", 100);
        int tuplesPerSecond = params.getInt("tuplesPerSecond", 50000);
        int numberOfTuples = params.getInt("numberOfTuples", 50000);
        int rampUpTimeSeconds = params.getInt("rampUpTimeSeconds", 0);
        int triggerIntervalMs = params.getInt("triggerIntervalMs", 0);
        int artificialDelay = params.getInt("artificialDelayMs", 0);
        Preconditions.checkArgument(triggerIntervalMs >= 0, "Trigger interval can't be negative.");

        // Logging frequency in #records for throughput calculations
        int logFreq = params.getInt("logFreq", 10000);

        StreamExecutionEnvironment env = getExecutionEnvironment(parallelism);

//        if (params.getBoolean("enableObjectReuse", true)) {
//            env.getConfig().enableObjectReuse();
//        }

        List<CampaignAd> campaignAds = generateCampaignMapping(numCampaigns);

        // Check here for correctness, in case of errors
        Map<String, String> campaignLookup = new HashMap<>();
        for (CampaignAd campaignAd : campaignAds) {
            campaignLookup.put(campaignAd.ad_id, campaignAd.campaign_id);
        }

        DataStreamSource<Event> source = env.addSource(new EventGenerator(campaignAds, numberOfTuples, artificialDelay));

        WindowedStream<Tuple3<String, String, Timestamp>, Tuple, TimeWindow> windowedEvents = source
                .flatMap(new ThroughputLogger(logFreq))
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.event_type.equals("view");
                    }
                })
                .flatMap(new StaticJoinMapper(campaignLookup))
                .assignTimestamps(new AdTimestampExtractor())
                .keyBy(0) // campaign_id
                .window(TumblingEventTimeWindows.of(windowMillis));

        // set tmp trigger to reduce latency. Leave it out to increase throughput
        if (triggerIntervalMs > 0) {
            windowedEvents.trigger(new EventAndProcessingTimeTrigger(triggerIntervalMs));
        }


        SingleOutputStreamOperator<WindowedCount> fold = windowedEvents.fold(new WindowedCount(null, "", 0, new Timestamp(0L)),
                new FoldFunction<Tuple3<String, String, Timestamp>, WindowedCount>() {
                    @Override
                    public WindowedCount fold(WindowedCount accumulator, Tuple3<String, String, Timestamp> value) throws Exception {
                        Timestamp lastUpdate;

                        if (accumulator.lastUpdate.getTime() < value.f2.getTime()) {
                            lastUpdate = value.f2;
                        } else {
                            lastUpdate = accumulator.lastUpdate;
                        }
                        accumulator.count += 1;
                        accumulator.lastUpdate = lastUpdate;
                        return accumulator;
                    }
                },
                new WindowFunction<WindowedCount, WindowedCount, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<WindowedCount> input, Collector<WindowedCount> out) throws Exception {
                        for (WindowedCount windowedCount : input) {
                            out.collect(new WindowedCount(
                                    new Timestamp(window.getStart()),
                                    (String) tuple.getField(0),
                                    windowedCount.count,
                                   windowedCount.lastUpdate));
                        }
                    }
                }
        );

        fold.print();

        env.execute("Flink Yahoo Benchmark");
    }

    /**
     * Generate in-memory tmp to campaign_id map. We generate 10 ads per campaign.
     */
    private static List<CampaignAd> generateCampaignMapping(int numCampaigns) {

        List<CampaignAd> campaignAds = new ArrayList<>();

        for (int i = 0; i < numCampaigns; i++) {

            String campaign = UUID.randomUUID().toString();

            for (int j = 0; j < 10; j++) {
                campaignAds.add(new CampaignAd(UUID.randomUUID().toString(), campaign));
            }
        }

        return campaignAds;
    }

    /**
     * Handle configuration of env here
     */
    private static StreamExecutionEnvironment getExecutionEnvironment(int parallelism) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (parallelism > 0) {
            executionEnvironment.setParallelism(parallelism);
        }

        return executionEnvironment;
    }
}
