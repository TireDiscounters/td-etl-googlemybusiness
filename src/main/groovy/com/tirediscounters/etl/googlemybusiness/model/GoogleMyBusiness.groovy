package com.tirediscounters.etl.googlemybusiness.model

public class GoogleMyBusiness {
    public List<StoreInsight> storeInsightList = new ArrayList<StoreInsight>();
    public List<StoreRating> storeRatingList = new ArrayList<StoreRating>();



    // Getter
    public List<StoreInsight> getStoreInsightList() {
        return storeInsightList;
    }

    // Setter
    public void setStoreInsightList(List<StoreInsight> newStoreInsightList) {
        this.storeInsightList = newStoreInsightList;
    }

    // Getter
    public List<StoreRating> getStoreRatingList() {
        return storeRatingList;
    }

    // Setter
    public void setStoreRatingList(List<StoreRating> newStoreRatingList) {
        this.storeRatingList = newStoreRatingList;
    }
}
