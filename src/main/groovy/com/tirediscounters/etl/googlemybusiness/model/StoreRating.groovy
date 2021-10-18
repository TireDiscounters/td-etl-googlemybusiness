package com.tirediscounters.etl.googlemybusiness.model

import com.tirediscounters.etl.common.model.DimETLRecord

class StoreRating extends DimETLRecord {
    String m_storeId
    String m_storeKey
    String m_storeAddress
    String m_date
    String m_dateKey
    String m_rating

    List<String> getJsonFieldList() {
        return ["store id", "store key", "store address", "date", "date key", "rating", "row_hash", "row_creation_timestamp"]
    }

    List<String> getHashFieldList() {
        return ["store id", "store key", "store address","date", "date key", "rating"]
    }

}
