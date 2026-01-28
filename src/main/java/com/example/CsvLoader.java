package com.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvLoader {

    // IMPORTANT: Create a DynamoDB table with this name.
    // The primary key should be a String named "station_cd".
    private static final String DYNAMODB_TABLE_NAME = "station_data_v2";

    private static final String LINE_CSV_PATH = "data/ekidata/line20250604free.csv";
    private static final String STATION_CSV_PATH = "data/ekidata/station20251211free.csv";

    public static void main(String[] args) {
        System.out.println("Starting CSV data load into DynamoDB...");

        try {
            // 1. Load lines into a map for easy lookup
            Map<String, String> lineCdToNameMap = new HashMap<>();
            try (Reader lineReader = new FileReader(LINE_CSV_PATH);
                 CSVParser lineParser = new CSVParser(lineReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                
                System.out.println("Loading lines from " + LINE_CSV_PATH);
                for (CSVRecord record : lineParser) {
                    lineCdToNameMap.put(record.get("line_cd"), record.get("line_name"));
                }
                System.out.println("Loaded " + lineCdToNameMap.size() + " lines.");
            }

            // 2. Prepare DynamoDB client
            DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                    .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();

            // 3. Read stations and batch write to DynamoDB
            System.out.println("Loading stations from " + STATION_CSV_PATH);
            List<WriteRequest> writeRequests = new ArrayList<>();
            int totalStations = 0;

            try (Reader stationReader = new FileReader(STATION_CSV_PATH);
                 CSVParser stationParser = new CSVParser(stationReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

                for (CSVRecord record : stationParser) {
                    totalStations++;
                    String stationCd = record.get("station_cd");
                    String lineCd = record.get("line_cd");
                    String lineName = lineCdToNameMap.get(lineCd);

                    // Skip if line name is not found
                    if (lineName == null) {
                        System.out.println("WARN: Could not find line name for line_cd: " + lineCd);
                        continue;
                    }

                    Map<String, AttributeValue> item = new HashMap<>();
                    item.put("station_cd", AttributeValue.builder().s(stationCd).build());
                    item.put("station_name", AttributeValue.builder().s(record.get("station_name")).build());
                    item.put("line_cd", AttributeValue.builder().s(lineCd).build());
                    item.put("line_name", AttributeValue.builder().s(lineName).build());
                    item.put("lon", AttributeValue.builder().n(record.get("lon")).build());
                    item.put("lat", AttributeValue.builder().n(record.get("lat")).build());

                    writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
                    
                    // DynamoDB batch write can handle up to 25 items at a time
                    if (writeRequests.size() == 25) {
                        batchWrite(dynamoDbClient, writeRequests);
                        writeRequests.clear();
                    }
                }
            }

            // Write any remaining items
            if (!writeRequests.isEmpty()) {
                batchWrite(dynamoDbClient, writeRequests);
            }

            System.out.println("Successfully processed " + totalStations + " station records.");
            System.out.println("Data loading complete.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void batchWrite(DynamoDbClient dynamoDbClient, List<WriteRequest> writeRequests) {
        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(Map.of(DYNAMODB_TABLE_NAME, writeRequests))
                .build();
        
        try {
            dynamoDbClient.batchWriteItem(batchWriteItemRequest);
            System.out.println("Wrote " + writeRequests.size() + " items to DynamoDB.");
        } catch (Exception e) {
            System.err.println("Error during batch write: " + e.getMessage());
        }
    }
}
