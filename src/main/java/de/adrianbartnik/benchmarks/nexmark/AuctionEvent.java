package de.adrianbartnik.benchmarks.nexmark;

import java.io.Serializable;

public class AuctionEvent implements Serializable {

    private final long timestamp;
    private final int auctionId;
    private final int personId;
    private final int itemId;
    private final double initialPrice;
    private final long start;
    private final long end;
    private final long categoryId;
    private final long ingestionTimestamp;

    public AuctionEvent(long timestamp, int auctionId, int itemId, int personId, double initialPrice, long categoryID, long start, long end) {
        this.timestamp = timestamp;
        this.auctionId = auctionId;
        this.personId = personId;
        this.itemId = itemId;
        this.initialPrice = initialPrice;
        this.categoryId = categoryID;
        this.start = start;
        this.end = end;
        this.ingestionTimestamp = System.currentTimeMillis();
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

    public Integer getItemId() {
        return itemId;
    }

    public Double getInitialPrice() {
        return initialPrice;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public long getIngestionTimestamp() {
        return ingestionTimestamp;
    }
}
