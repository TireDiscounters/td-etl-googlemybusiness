package com.tirediscounters.etl.googlemybusiness

import com.tirediscounters.etl.common.APIETLApplication
import com.tirediscounters.etl.common.model.StoreKey
import com.tirediscounters.etl.dbreader.RedshiftReader
import com.tirediscounters.etl.googlemybusiness.model.GoogleMyBusiness
import com.tirediscounters.etl.googlemybusiness.model.Store
import com.tirediscounters.etl.googlemybusiness.model.StoreInsight
import com.tirediscounters.etl.googlemybusiness.model.StoreRating
import com.tirediscounters.utils.ProgramArguments
import com.tirediscounters.utils.ProgramEnvironment
import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.core.env.Environment
import javax.sql.DataSource
import groovy.json.JsonSlurper

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@SpringBootApplication (
        excludeName = ["org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
                "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration"
        ])
class GoogleMyBusinessETLApplication extends APIETLApplication implements CommandLineRunner{
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleMyBusinessETLApplication);

    @Autowired
    private Environment environment;

    @Autowired
    @Qualifier("dataWarehouseDataSource")
    private DataSource m_dataWarehouseDataSource

    private RedshiftReader redshiftReader
    private String dataWarehouseSchema

    private String startDateString
    private String endDateString
    private String startDate
    private String endDate
    private String insightDate
    private String gmbURL
    private String gmbToken
    private String gmbGID
    private String aggregate
    private String m_objectKeyPrefix
    private String accountId = new String();
    private List<Store> storeList = new ArrayList<Store>();
    private GoogleMyBusiness googleMyBusiness = new GoogleMyBusiness();

    static void main(String[] args){
        SpringApplication.run(GoogleMyBusinessETLApplication, args)
    }

    @Override
    public void run(String... args) {
        ProgramArguments programArguments = new ProgramArguments(args)
        ProgramEnvironment programEnvironment = new ProgramEnvironment(environment)

        System.setProperty("com.sun.xml.ws.transport.http.client.HttpTransportPipe.dump", "true");
        System.setProperty("com.sun.xml.internal.ws.transport.http.client.HttpTransportPipe.dump", "true");
        System.setProperty("com.sun.xml.ws.transport.http.HttpAdapter.dump", "true");
        System.setProperty("com.sun.xml.internal.ws.transport.http.HttpAdapter.dump", "true");
        System.setProperty("com.sun.xml.internal.ws.transport.http.HttpAdapter.dumpTreshold", "999999");

        init(programEnvironment, programArguments)

        loadStoreKeyMap()

        getAccountId();
        getLocationIds(accountId);
        getLocationInsights(accountId, storeList)

        // upload list of Store Insights and Ratings to Redshift
        processStoreInsights(googleMyBusiness.getStoreInsightList())
        processStoreRatings(googleMyBusiness.getStoreRatingList())
    }

    public String getAccountId(){
        URL url = new URL(gmbURL + '/accounts')

        HttpURLConnection connection = (HttpURLConnection) url.openConnection()
        connection.setRequestProperty("Authorization", gmbToken)
        connection.setRequestProperty("gid", gmbGID)
        connection.setRequestProperty("Accept", "*/*")
        connection.setRequestMethod("GET")

        Integer responseCode = connection.getResponseCode()
        if (200 <= responseCode && responseCode <= 399) {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            StringBuilder sb = new StringBuilder();
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                sb.append(strCurrentLine + "\n");
            }
            br.close();
            String responseBody = sb.toString();
            def responseJson = new JsonSlurper().parseText(responseBody)

            for (Object entry : responseJson) {
                if (entry.accountName == "Tire Discounters")
                {
                    accountId = entry.accountId
                }
            }

        } else {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                println(strCurrentLine);
            }
        }
        return accountId;
    }

    public List<Store> getLocationIds (String accountId){
        URL url = new URL(gmbURL + '/locations/account/' + accountId)

        HttpURLConnection connection = (HttpURLConnection) url.openConnection()
        connection.setRequestProperty("Authorization", gmbToken)
        connection.setRequestProperty("gid", gmbGID)
        connection.setRequestProperty("Accept", "*/*")
        connection.setRequestMethod("GET")

        Integer responseCode = connection.getResponseCode()
        if (200 <= responseCode && responseCode <= 399) {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            StringBuilder sb = new StringBuilder();
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                sb.append(strCurrentLine + "\n");
            }
            br.close();
            String responseBody = sb.toString();
            def responseJson = new JsonSlurper().parseText(responseBody)
            for(Object entry : responseJson) {
                Store storeObject = new Store();
                storeObject.locationId = entry.locationId
                storeObject.storeId = entry.storeCode
                int storeId = Integer.parseInt(entry.storeCode)
                StoreKey _storeKey = getStoreKey(storeId, LocalDate.now())
                storeObject.storeKey = _storeKey != null ? _storeKey.key : "000";
                storeObject.storeAddress = entry.address.addressLines[0]

                storeList.add(storeObject)
            }
        } else {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                println(strCurrentLine);
            }
        }

        return storeList;
    }

    public GoogleMyBusiness getLocationInsights(String accountId, List<Store> storeList){
        URL url = new URL(gmbURL + '/insights/?endDateTime=' + endDate + '&startDateTime=' + startDate)
        for (Store store : storeList) {

            String accountLocString = "{\"locations\": [\"${accountId}/${store.locationId}\"]}"

            HttpURLConnection connection = (HttpURLConnection) url.openConnection()
            connection.setRequestProperty("Authorization", gmbToken)
            connection.setRequestProperty("gid", gmbGID)
            connection.setRequestProperty("aggregate", "minute")
            connection.setRequestProperty("Accept", "*/*")
            connection.setRequestProperty("content-type", "application/json");
            connection.setDoOutput(true)
            connection.setRequestMethod("POST")
            OutputStream os = connection.getOutputStream();
            OutputStreamWriter wr = new OutputStreamWriter(os, "UTF-8");
            wr.write(accountLocString);
            wr.flush();
            wr.close();
            os.close();
            connection.connect();

            Integer responseCode = connection.getResponseCode()
            if (200 <= responseCode && responseCode <= 399) {
                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

                StringBuilder sb = new StringBuilder();
                String strCurrentLine;
                while ((strCurrentLine = br.readLine()) != null) {
                    sb.append(strCurrentLine + "\n");
                }
                br.close();
                String responseBody = sb.toString();
                def responseJson = new JsonSlurper().parseText(responseBody)
                for(Object entry : responseJson) {

                    if(entry.value != "ok") {
                        StoreInsight locationInsight = new StoreInsight();

                        locationInsight.m_storeId = store.storeId
                        locationInsight.m_storeKey = store.storeKey
                        locationInsight.m_storeAddress = store.storeAddress
                        locationInsight.m_date = insightDate
                        locationInsight.m_dateKey = insightDate.replaceAll("[\\s\\-()]", "")
                        locationInsight.m_directionRequests = entry.value.actionStats.ACTIONS_DRIVING_DIRECTIONS.total.numeric
                        locationInsight.m_mobilePhoneCalls = entry.value.actionStats.ACTIONS_PHONE.total.numeric
                        locationInsight.m_websiteVisits = entry.value.actionStats.ACTIONS_WEBSITE.total.numeric
                        locationInsight.m_localPostActions = entry.value.localPostStats.LOCAL_POST_ACTIONS_CALL_TO_ACTION.total.numeric
                        locationInsight.m_localPostViews = entry.value.localPostStats.LOCAL_POST_VIEWS_SEARCH.total.numeric
                        locationInsight.m_customerPhotoCount = entry.value.photoStats.PHOTOS_COUNT_CUSTOMERS.total.numeric
                        locationInsight.m_merchantPhotoCount = entry.value.photoStats.PHOTOS_COUNT_MERCHANT.total.numeric
                        locationInsight.m_customerPhotoViews = entry.value.photoStats.PHOTOS_VIEWS_CUSTOMERS.total.numeric
                        locationInsight.m_merchantPhotoViews = entry.value.photoStats.PHOTOS_VIEWS_MERCHANT.total.numeric
                        locationInsight.m_directSearches = entry.value.searchStats.QUERIES_DIRECT.total.numeric
                        locationInsight.m_discoverySearches = entry.value.searchStats.QUERIES_INDIRECT.total.numeric
                        locationInsight.m_mapViews = entry.value.viewStats.VIEWS_MAPS.total.numeric
                        locationInsight.m_searchViews = entry.value.viewStats.VIEWS_SEARCH.total.numeric

                        googleMyBusiness.storeInsightList.add(locationInsight)

                        for(Object rate : entry.value.aggregate) {
                            StoreRating locationRating = new StoreRating();

                            locationRating.m_storeId = store.storeId
                            locationRating.m_storeKey = store.storeKey
                            locationRating.m_storeAddress = store.storeAddress
                            locationRating.m_date = insightDate
                            locationRating.m_dateKey = insightDate.replaceAll("[\\s\\-()]", "");
                            locationRating.m_rating = rate.rating

                            googleMyBusiness.storeRatingList.add(locationRating)
                        }
                    }
                }
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                String strCurrentLine;
                while ((strCurrentLine = br.readLine()) != null) {
                    println(strCurrentLine);
                }
            }

        }

        return googleMyBusiness;
    }

    public void processStoreInsights(List<StoreInsight> storeInsightList){
        final ExecutorService executor = Executors.newFixedThreadPool(12)

        if (storeInsightList == null || storeInsightList.size() == 0){
            LOGGER.info("No store insights in the list.")
            System.exit(0)
        }

        int count = 0
        final List<StoreInsight> buffer = new ArrayList<>()
        String tableName = "store_insights"
        String objectKeyPrefix = m_objectKeyPrefix + tableName + "/"
        storeInsightList.each { final storeInsight ->
            if (batchIsFull(count)) {
                LOGGER.info("$count store insights have been extracted and translated")

                // dump the buffer content into a new collection
                List<StoreInsight> recordList = new ArrayList<>()
                buffer.each { recordList.add(it) }

                // clear the buffer
                buffer.clear()

                // write the records to a S3 object
                executor.execute {
                    createS3ObjectByTable(recordList, objectKeyPrefix, tableName)
                }
                count = 0
            }

            buffer.add(storeInsight)
            count ++
        }

        if (buffer.size() > 0) {
            // if the buffer is not empty, write its content to a S3 object
            executor.execute {
                createS3ObjectByTable(buffer, objectKeyPrefix, tableName)
            }
        }

        executor.shutdown()

    }

    public void processStoreRatings(List<StoreRating> storeRatingList){
        final ExecutorService executor = Executors.newFixedThreadPool(12)

        if (storeRatingList == null || storeRatingList.size() == 0){
            LOGGER.info("No store ratings in the list.")
            System.exit(0)
        }

        int count = 0
        final List<StoreRating> buffer = new ArrayList<>()
        String tableName = "store_ratings"
        String objectKeyPrefix = m_objectKeyPrefix + tableName + "/"
        storeRatingList.each { final storeRating ->
            if (batchIsFull(count)) {
                LOGGER.info("$count store ratings have been extracted and translated")

                // dump the buffer content into a new collection
                List<StoreRating> recordList = new ArrayList<>()
                buffer.each { recordList.add(it) }

                // clear the buffer
                buffer.clear()

                // write the records to a S3 object
                executor.execute {
                    createS3ObjectByTable(recordList, objectKeyPrefix, tableName)
                }
                count = 0
            }

            buffer.add(storeRating)
            count ++
        }

        if (buffer.size() > 0) {
            // if the buffer is not empty, write its content to a S3 object
            executor.execute {
                createS3ObjectByTable(buffer, objectKeyPrefix, tableName)
            }
        }

        executor.shutdown()

    }

    /**
     * Retrieves store key information from data warehouse
     * @return
     */
    def loadStoreKeyMap() {
        String query = """
                SELECT key, "store id", row_effective_date, row_expiration_date
                FROM ${dataWarehouseSchema}.store
                """

        LOGGER.info("Start retrieving store key information ...")

        Sql sql = new Sql(m_dataWarehouseDataSource)
        sql.query(query) {rs ->
            while (rs.next()) {
                int storeId = rs.getInt("store id")
                long key = rs.getLong("key")
                LocalDate effectiveDate = rs.getDate("row_effective_date")?.toLocalDate()
                LocalDate expirationDate = rs.getDate("row_expiration_date")?.toLocalDate()

                StoreKey storeKey = new StoreKey(storeId, key, effectiveDate, expirationDate)
                if (storeKeyMap.get(storeId) == null) {  // first encounter of that store id.
                    storeKeyMap.put(storeId, new HashSet<StoreKey>())
                }
                storeKeyMap.get(storeId).add(storeKey)
            }
        }
        LOGGER.info("Finished retrieving store key information ...")
    }

    StoreKey getStoreKey(int storeId, LocalDate date) {
        Set<StoreKey> keySet = storeKeyMap.get(storeId)
        if (keySet == null || keySet.size() == 0) {
            String error = "There is not key for store id $storeId."
            LOGGER.error(error)
            return null
        }
        boolean found = false;
        StoreKey result
        keySet.each{key ->
            if ((key.effectiveDate?.isEqual(date) || key.effectiveDate?.isBefore(date))  && key.expirationDate?.isAfter(date)) {
                found = true
                result = key
            }
        }
        if (!found) {
            String error = "There is not key covering date $date for store id $storeId."
            LOGGER.error(error)
            throw new RuntimeException(error)
        }
        return result
    }

    protected init(ProgramEnvironment programEnvironment, ProgramArguments programArguments) {
        super.init(programEnvironment)

        final String dataWarehouseSchema = programEnvironment.getRequiredPropertyAsString("data.warehouse.schema")

        this.m_batchSize = programEnvironment.getRequiredPropertyAsInteger("batch.size")
        this.redshiftReader = new RedshiftReader(m_dataWarehouseDataSource, dataWarehouseSchema)

        m_objectKeyPrefix = programEnvironment.getRequiredPropertyAsString('aws.s3.key.prefix')

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        if (programArguments.getArgumentAsString("aggregate").isPresent()) {
            aggregate = programArguments.getArgumentAsString("aggregate").get()
        }

            if (programArguments.getArgumentAsLocalDate("startDate").isPresent()) {
                startDateString = programArguments.getRequiredArgumentAsLocalDate("startDate")
                endDateString = programArguments.getRequiredArgumentAsLocalDate("endDate")
                startDate = startDateString + "T00:00"
                endDate = endDateString + "T23:59:59.999999999"
                insightDate = startDateString
            } else {
                LocalDateTime now = LocalDateTime.now().minus(4, ChronoUnit.DAYS);
                insightDate = now.format(formatter)
                startDate = now.with(LocalTime.MIN);
                endDate = now.with(LocalTime.MAX);
            }

        if (programArguments.getArgumentAsString("localPath").isPresent()) {
            this.m_localPath = programArguments.getArgumentAsString("localPath").get()
        }

        gmbURL = programEnvironment.getRequiredPropertyAsString("googlemybusiness.url")
        gmbToken = programEnvironment.getRequiredPropertyAsString("googlemybusiness.token")
        gmbGID = programEnvironment.getRequiredPropertyAsString("googlemybusiness.gid")
    }
}
