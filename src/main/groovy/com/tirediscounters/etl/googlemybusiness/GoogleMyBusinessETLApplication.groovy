package com.tirediscounters.etl.googlemybusiness

import com.tirediscounters.etl.common.APIETLApplication
import com.tirediscounters.etl.dbreader.RedshiftReader
import com.tirediscounters.etl.googlemybusiness.model.StoreInsight
import com.tirediscounters.utils.ProgramArguments
import com.tirediscounters.utils.ProgramEnvironment
import org.json.JSONObject
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

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
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


    private String startDate
    private String endDate
    private String gmbURL
    private String gmbToken
    private String gmbGID
    private String accountId = new String();
    private List<String> locationMap = new ArrayList<String>();
    private List<StoreInsight> storeInsightList = new ArrayList<StoreInsight>();

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

        this.m_batchSize = programEnvironment.getRequiredPropertyAsInteger("batch.size")


        //todo add ability to pass in month and run for each day
        if (programArguments.getArgumentAsLocalDate("firstDay").isPresent()) {
            startDate = programArguments.getArgumentAsLocalDate("startDate").with(LocalTime.MIN)
            endDate = programArguments.getRequiredArgumentAsLocalDate("endDate").with(LocalTime.MAX)
        } else {
            LocalDateTime now = LocalDateTime.now();

            startDate = now.with(LocalTime.MIN);
            endDate = now.with(LocalTime.MAX);
        }

        init(programEnvironment, programArguments)

        getAccountId();
        getLocationIds(accountId);
        getLocationInsights(accountId, locationMap)

        // upload list of Store Insights to Redshift
        processStoreInsights(storeInsightList)
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

            accountId = responseJson[0].accountId
        } else {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                println(strCurrentLine);
            }
        }
        return accountId;
    }

    public List<String> getLocationIds (String accountId){
        //List<String> locationMap = new ArrayList<String>();
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
                locationMap.add(entry.locationId)
            }
        } else {
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            String strCurrentLine;
            while ((strCurrentLine = br.readLine()) != null) {
                println(strCurrentLine);
            }
        }

        return locationMap;
    }

    public List<StoreInsight> getLocationInsights(String accountId, List<String> locationMap){

        SimpleDateFormat dwFormat = new SimpleDateFormat("yyyy-MM-dd");
        URL url = new URL(gmbURL + '/insights/?' + endDate + '&' + startDate)

        for (String locationId : locationMap) {
            JSONObject bodyJson   = new JSONObject();
            String accountLocString = "[" + accountId + "/" + locationId + "]"
            bodyJson.put("Locations",accountLocString)

            HttpURLConnection connection = (HttpURLConnection) url.openConnection()
            connection.setRequestProperty("Authorization", gmbToken)
            connection.setRequestProperty("gid", gmbGID)
            connection.setRequestProperty("Accept", "*/*")
            connection.setDoOutput(true)
            connection.setRequestMethod("POST")

            OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
            wr.write(bodyJson.toString());
            wr.flush();

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
                    StoreInsight locationInsight = new StoreInsight();

                    locationInsight.m_storeId = entry.locationId
                    locationInsight.m_storeName = entry.locationId
                    locationInsight.m_date = dwFormat.format(startDate)
                    locationInsight.m_directionRequests = entry.directionRequests
                    locationInsight.m_mobilePhoneCalls = entry.mobilePhoneCalls
                    locationInsight.m_websiteVisits = entry.websiteVisits
                    locationInsight.m_rating = entry.rating
                    locationInsight.m_localPostActions = entry.localPostActions
                    locationInsight.m_localPostViews = entry.localPostViews
                    locationInsight.m_customerPhotoCount = entry.customerPhotoCount
                    locationInsight.m_merchantPhotoCount = entry.merchantPhotoCount
                    locationInsight.m_customerPhotoViews = entry.customerPhotoViews
                    locationInsight.m_merchantPhotoViews = entry.merchantPhotoViews
                    locationInsight.m_directSearches = entry.directSearches
                    locationInsight.m_discoverySearches = entry.discoverySearches
                    locationInsight.m_mapViews = entry.mapViews
                    locationInsight.m_searchViews = entry.searchViews

                    storeInsightList.add(locationInsight)
                }
            } else {
                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                String strCurrentLine;
                while ((strCurrentLine = br.readLine()) != null) {
                    println(strCurrentLine);
                }
            }
        }

        return storeInsightList;
    }

    public void processStoreInsights(List<StoreInsight> storeInsightList){

        final ExecutorService executor = Executors.newFixedThreadPool(12)

        if (storeInsightList == null || storeInsightList.size() == 0){
            LOGGER.info("No store insights in the list.")
            System.exit(0)
        }

        int count = 0
        final List<StoreInsight> buffer = new ArrayList<>()
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
                    createS3Object(recordList)
                }
                count = 0
            }

            buffer.add(storeInsight)
            count ++
        }

        if (buffer.size() > 0) {
            // if the buffer is not empty, write its content to a S3 object
            executor.execute {
                createS3Object(buffer)
            }
        }

        executor.shutdown()

    }

    protected init(ProgramEnvironment programEnvironment, ProgramArguments programArguments) {
        super.init(programEnvironment)

        final String dataWarehouseSchema = programEnvironment.getRequiredPropertyAsString("data.warehouse.schema")

        this.redshiftReader = new RedshiftReader(m_dataWarehouseDataSource, dataWarehouseSchema)

        if (programArguments.getArgumentAsLocalDate("firstDay").isPresent()) {
            startDate = programArguments.getArgumentAsLocalDate("startDate").with(LocalTime.MIN)
            endDate = programArguments.getRequiredArgumentAsLocalDate("endDate").with(LocalTime.MAX)
        } else {
            LocalDateTime now = LocalDateTime.now();

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
