package de.adrianbartnik.flink.objects;

public class CampaignAd {
    public final String ad_id;
    public final String campaign_id;

    public CampaignAd(String ad_id, String campaign_id) {
        this.ad_id = ad_id;
        this.campaign_id = campaign_id;
    }
}