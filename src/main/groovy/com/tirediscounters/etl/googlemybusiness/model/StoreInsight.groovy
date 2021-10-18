package com.tirediscounters.etl.googlemybusiness.model

import com.tirediscounters.etl.common.model.DimETLRecord

class StoreInsight extends DimETLRecord {
    String m_storeId
    String m_storeKey
    String m_storeAddress
    String m_date
    String m_dateKey
    String m_directionRequests
    String m_mobilePhoneCalls
    String m_websiteVisits
    String m_localPostActions
    String m_localPostViews
    String m_customerPhotoCount
    String m_merchantPhotoCount
    String m_customerPhotoViews
    String m_merchantPhotoViews
    String m_directSearches
    String m_discoverySearches
    String m_mapViews
    String m_searchViews

    List<String> getJsonFieldList() {
        return ["store id", "store key", "store address", "date", "date key", "direction requests", "mobile phone calls", "website visits",
                "local post actions", "local post views", "customer photo count", "merchant photo count", "customer photo views",
                "merchant photo views", "direct searches", "discovery searches", "map views", "search views",
                "row_hash", "row_creation_timestamp"]
    }

    List<String> getHashFieldList() {
        return ["store id", "store key", "store address", "date", "date key", "direction requests", "mobile phone calls", "website visits",
                "local post actions", "local post views", "customer photo count", "merchant photo count", "customer photo views",
                "merchant photo views", "direct searches", "discovery searches", "map views", "search views"]
    }

}
