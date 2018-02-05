package de.adrianbartnik.operator;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class JoiningNewUsersWithAuctionsFunction extends RichJoinFunction<NewPersonEvent, AuctionEvent, Tuple5<Long, Integer, String, Long, Integer>> {

    /**
     * Join Auction and Person on name and return the Persons name as well as ID. Finding every person that opened an auction in the last x minutes
     */
    @Override
    public Tuple5<Long, Integer, String, Long, Integer> join(NewPersonEvent person, AuctionEvent auction) {

        Long latencyTimestamp;

        if (auction.getIngestionTimestamp() > person.getIngestionTimestamp()) {
            latencyTimestamp = auction.getIngestionTimestamp();
        } else {
            latencyTimestamp = person.getIngestionTimestamp();
        }

        Integer person_id = person.getPersonId();
        String person_name = person.getName();
        Long timestamp = System.currentTimeMillis();

        return new Tuple5<>(timestamp, person_id, person_name, latencyTimestamp, 2);
    }
}

