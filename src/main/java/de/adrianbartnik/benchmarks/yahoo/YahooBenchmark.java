package de.adrianbartnik.benchmarks.yahoo;

import de.adrianbartnik.benchmarks.yahoo.generator.EventGenerator;
import de.adrianbartnik.benchmarks.yahoo.objects.CampaignAd;
import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.JoinedEventWithCampaign;
import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.WindowedCount;
import de.adrianbartnik.benchmarks.yahoo.operators.AdTimestampExtractor;
import de.adrianbartnik.benchmarks.yahoo.operators.EventAndProcessingTimeTrigger;
import de.adrianbartnik.benchmarks.yahoo.operators.IndependentJoinMapper;
import de.adrianbartnik.benchmarks.yahoo.operators.StaticJoinMapper;
import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.sink.latency.YahooWindowCountLatencySink;
import de.adrianbartnik.source.socket.IndependentYahooEventParallelSocketSource;
import de.adrianbartnik.source.socket.YahooEventParallelSocketSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

/**
 * Modified based on https://github.com/dataArtisans/yahoo-streaming-benchmark/blob/d8381f473ab0b72e33469d2b98ed1b77317fe96d/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyFlinkWindows.java
 */
public class YahooBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(YahooBenchmark.class);

    public static final String JOB_NAME = "Flink Yahoo Benchmark";

    public static void main(String args[]) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelism = params.getInt("parallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");
        final String output_path = params.get("path", "yahooBenchmarkOutput");

        if (hostnames_string == null || hostnames_string.isEmpty() || ports_string == null || ports_string.isEmpty()) {
            throw new IllegalArgumentException("Hostname and Ports must not be empty");
        }

        List<String> hostnames = Arrays.asList(hostnames_string.split(","));
        List<String> separated_ports = Arrays.asList(ports_string.split(","));

        List<Integer> ports = new ArrayList<>();
        for (String port : separated_ports) {
            ports.add(Integer.valueOf(port));
        }

        if (ports.size() != hostnames.size()) {
            throw new IllegalArgumentException("Hostname and Ports must be of equal size");
        }

        final int sourceParallelism = hostnames.size();
        for (int i = 0; i < hostnames.size(); i++) {
            LOG.debug("Connecting to socket {}:{}", hostnames.get(i), ports.get(i));
        }

        Time windowMillis = Time.milliseconds(params.getLong("windowMillis", 10000));
        int numCampaigns = params.getInt("numCampaigns", 100);
        int numberOfTuples = params.getInt("numberOfTuples", 50000);
        int triggerIntervalMs = params.getInt("triggerIntervalMs", 0);
        int artificialDelay = params.getInt("artificialDelayMs", 0);
        String generator = params.get("generator", "independent");

        Preconditions.checkArgument(parallelism > 0, "Parallelism needs to be tmp positive integer.");
        Preconditions.checkArgument(triggerIntervalMs >= 0, "Trigger interval can't be negative.");

        StreamExecutionEnvironment environment =
                new FlinkJobFactory(args, true, true).setupExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(parallelism);

        if (params.getBoolean("enableObjectReuse", true)) {
            environment.getConfig().enableObjectReuse();
        }

        WindowedStream<JoinedEventWithCampaign, String, TimeWindow> windowedEvents;

        switch (generator.toLowerCase()) {
            case "independent":

                windowedEvents =
                        new IndependentYahooEventParallelSocketSource(hostnames, ports, sourceParallelism).createSource(args, environment)
                                .filter(value -> value.eventType.equals("view"))
                                .map(new IndependentJoinMapper<>())
                                .assignTimestampsAndWatermarks(new AdTimestampExtractor())
                                .keyBy(value -> value.campaignId)
                                .window(TumblingEventTimeWindows.of(windowMillis));

                break;
            case "uuid":

                windowedEvents =
                        new YahooEventParallelSocketSource(hostnames, ports, sourceParallelism).createSource(args, environment)
                                .filter(value -> value.eventType.equals("view"))
                                .map(new IndependentJoinMapper<>())
                                .assignTimestampsAndWatermarks(new AdTimestampExtractor())
                                .keyBy(value -> value.campaignId)
                                .window(TumblingEventTimeWindows.of(windowMillis));

                break;

            case "flinkSource":
                List<CampaignAd> campaignAds = generateCampaignMapping(numCampaigns);

                // Check here for correctness, in case of errors
                Map<String, String> campaignLookup = new HashMap<>();
                for (CampaignAd campaignAd : campaignAds) {
                    campaignLookup.put(campaignAd.ad_id, campaignAd.campaign_id);
                }

                windowedEvents = environment
                        .addSource(new EventGenerator(campaignAds, numberOfTuples, artificialDelay))
                        .filter(value -> value.eventType.equals("view"))
                        .map(new StaticJoinMapper(campaignLookup))
                        .assignTimestampsAndWatermarks(new AdTimestampExtractor())
                        .keyBy(value -> value.campaignId)
                        .window(TumblingEventTimeWindows.of(windowMillis));

                break;
            default:
                throw new IllegalArgumentException("No generator for '" + generator + '"');
        }


        // set tmp trigger to reduce latency. Leave it out to increase throughput
        if (triggerIntervalMs > 0) {
            windowedEvents.trigger(new EventAndProcessingTimeTrigger(triggerIntervalMs));
        }


        SingleOutputStreamOperator<WindowedCount> fold = windowedEvents.fold(
                new WindowedCount(null, "", 0, new Timestamp(0L)), (accumulator, value) -> {
                    Timestamp lastUpdate;

                    if (accumulator.lastUpdate.getTime() < value.eventTime.getTime()) {
                        lastUpdate = value.eventTime;
                    } else {
                        lastUpdate = accumulator.lastUpdate;
                    }
                    accumulator.count += 1;
                    accumulator.lastUpdate = lastUpdate;
                    return accumulator;
                },
                (campaignId, window, input, out) -> {
                    for (WindowedCount windowedCount : input) {
                        out.collect(new WindowedCount(
                                new Timestamp(window.getStart()),
                                campaignId,
                                windowedCount.count,
                                windowedCount.lastUpdate));
                    }
                }
        );

        new YahooWindowCountLatencySink(sinkParallelism, output_path).createSink(args, fold);

        environment.execute(JOB_NAME);
    }

    /**
     * Generate in-memory tmp to campaignId map. We generate 10 ads per campaign.
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
}
