package de.adrianbartnik.operator;

import de.adrianbartnik.benchmarks.nexmark.AuctionEvent;
import de.adrianbartnik.benchmarks.nexmark.NewPersonEvent;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoiningNewUsersWithAuctionsCoGroupFunction extends RichCoGroupFunction<NewPersonEvent, AuctionEvent, Tuple6<Long, Long, Long, Long, Long, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(JoiningNewUsersWithAuctionsCoGroupFunction.class);

    /**
     * CoGroups Auction and Person on person id and return the Persons name as well as ID.
     * Finding every person that created a new auction.
     */
    @Override
    public void coGroup(Iterable<NewPersonEvent> persons,
                        Iterable<AuctionEvent> auctions,
                        Collector<Tuple6<Long, Long, Long, Long, Long, String>> out) {

        LOG.debug("{} was called with {} and {}", this.getClass(), persons, auctions);

        boolean receivedPerson = false;

        for (NewPersonEvent person : persons) {

            if (receivedPerson) {
                LOG.debug("{} was called with {} and {}", this.getClass(), persons, auctions);
                throw new IllegalStateException("Only expects to receive one person as id's should be unique");
            }

            Long personCreationTimestamp = person.getTimestamp();
            Long personIngestionTimestamp = person.getIngestionTimestamp();

            long personId = person.getPersonId();
            String personName = person.getName();

            for (AuctionEvent auction : auctions) {
                Long auctionCreationTimestamp = auction.getTimestamp();
                Long auctionIngestionTimestamp = auction.getIngestionTimestamp();

                out.collect(new Tuple6<>(
                        personCreationTimestamp,
                        personIngestionTimestamp,
                        auctionCreationTimestamp,
                        auctionIngestionTimestamp,
                        personId,
                        personName));
            }

            receivedPerson = true;
        }
    }
}

