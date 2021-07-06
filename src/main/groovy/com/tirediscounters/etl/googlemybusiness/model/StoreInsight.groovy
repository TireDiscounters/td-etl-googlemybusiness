package com.tirediscounters.etl.googlemybusiness.model

import com.tirediscounters.etl.common.model.DimETLRecord

class StoreInsight extends DimETLRecord {
    String m_storeId
    String m_storeName
    String m_directionRequests
    String m_mobilePhoneCalls
    String m_websiteVisits
    String m_rating
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
}
