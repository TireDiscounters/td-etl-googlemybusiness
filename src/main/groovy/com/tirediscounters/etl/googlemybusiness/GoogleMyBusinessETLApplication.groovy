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

import java.time.LocalDateTime
import java.time.LocalTime

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
        final String dataWarehouseSchema = programEnvironment.getRequiredPropertyAsString("data.warehouse.schema")

        redshiftReader = new RedshiftReader(m_dataWarehouseDataSource, dataWarehouseSchema)

        LocalDateTime now = LocalDateTime.now();

        startDate = now.with(LocalTime.MIN);
        endDate = now.with(LocalTime.MAX);

        gmbURL = programEnvironment.getRequiredPropertyAsString("googlemybusiness.url")
        gmbToken = programEnvironment.getRequiredPropertyAsString("googlemybusiness.token")
        gmbGID = programEnvironment.getRequiredPropertyAsString("googlemybusiness.gid")

        if (programArguments.getArgumentAsString("localPath").isPresent()) {
            this.m_localPath = programArguments.getArgumentAsString("localPath").get()
        }

        init(programEnvironment)

        getAccountId();
        getLocationIds(accountId);
        getLocationInsights(accountId, locationMap)

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
            locationMap.add(responseJson.locationId)
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
        //List<StoreInsight> storeInsightList = new ArrayList<StoreInsight>();
        URL url = new URL(gmbURL + '/insights/?' + endDate + '&' + startDate)

        for (String entry : locationMap) {
            JSONObject bodyJson   = new JSONObject();
            bodyJson.put(accountId);
            bodyJson.put(entry);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection()
            connection.setRequestProperty("Authorization", gmbToken)
            connection.setRequestProperty("gid", gmbGID)
            connection.setRequestProperty("Accept", "*/*")
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
                storeInsightList.add(responseJson)
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
}
