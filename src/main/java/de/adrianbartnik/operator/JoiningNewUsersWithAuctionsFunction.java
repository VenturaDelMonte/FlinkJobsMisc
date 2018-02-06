package de.adrianbartnik.operator;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public class JoiningNewUsersWithAuctionsFunction extends RichJoinFunction<NewPersonEvent, AuctionEvent, Tuple6<Long, Long, Long, Long, Long, String>> {



    /**
     * Join Auction and Person on person id and return the Persons name as well as ID.
     * Finding every person that created a new auction.
     */
    @Override
    public Tuple6<Long, Long, Long, Long, Long, String> join(NewPersonEvent person, AuctionEvent auction) {

        Long personCreationTimestamp = person.getTimestamp();
        Long personIngestionTimestamp = person.getIngestionTimestamp();

        Long auctionCreationTimestamp = person.getTimestamp();
        Long auctionIngestionTimestamp = person.getIngestionTimestamp();

        long personId = person.getPersonId();
        String personName = person.getName();

        return new Tuple6<>(personCreationTimestamp, personIngestionTimestamp, auctionCreationTimestamp, auctionIngestionTimestamp, personId, personName);
    }
}

