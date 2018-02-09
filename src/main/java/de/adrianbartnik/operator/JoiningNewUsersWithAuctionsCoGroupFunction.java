package de.adrianbartnik.operator;

import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.data.nexmark.NewPersonEvent;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoiningNewUsersWithAuctionsCoGroupFunction extends RichCoGroupFunction<NewPersonEvent, AuctionEvent, Query8WindowOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(JoiningNewUsersWithAuctionsCoGroupFunction.class);

    /**
     * CoGroups Auction and Person on person id and return the Persons name as well as ID.
     * Finding every person that created a new auction.
     */
    @Override
    public void coGroup(Iterable<NewPersonEvent> persons,
                        Iterable<AuctionEvent> auctions,
                        Collector<Query8WindowOutput> out) {

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

                out.collect(new Query8WindowOutput(
                        System.currentTimeMillis(),
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

