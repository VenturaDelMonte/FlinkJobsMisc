package de.adrianbartnik.benchmarks.nexmark;

import java.io.Serializable;

public class BidEvent implements Serializable {

    private long timestamp;
    private int auctionId;
    private int personId;
    private int bidId;
    private double bid;

    public BidEvent(long timestamp, int auctionId, int personId, int bidId, double bid) {
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.bidId = bidId;
        this.bid = bid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Integer getAuctionId() {
        return auctionId;
    }

    public Integer getPersonId() {
        return personId;
    }

    public Integer getBidId() {
        return bidId;
    }

    public Double getBid() {
        return bid;
    }
}