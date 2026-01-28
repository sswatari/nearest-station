package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class NearestStationHandler implements RequestHandler<Map<String, Object>, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();

    // The Station record now holds the OSM ID from Overpass.
    private record Station(long id, String name, double lat, double lon, String line, double distance) {}

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) event.get("queryStringParameters");
            if (params == null) return createErrorResponse("No parameters provided.");
            
            double lat, lng;
            int limit;
            try {
                lat = Double.parseDouble(params.getOrDefault("lat", "0.0"));
                lng = Double.parseDouble(params.getOrDefault("lng", "0.0"));
                limit = Integer.parseInt(params.getOrDefault("limit", "5"));
                if (!params.containsKey("lat") || !params.containsKey("lng")) {
                    return createErrorResponse("Please specify lat and lng parameters.");
                }
            } catch (NumberFormatException e) {
                return createErrorResponse("Invalid parameter format for lat, lng, or limit.");
            }

            // Service to interact with our pre-populated station data
            StationDataService stationDataService = new StationDataService(context);

            // 1. Find station candidates from OpenStreetMap data
            List<Station> stationsFromOverpass = findStationsViaOverpass(lat, lng, limit, context);

            // 2. Enrich these candidates with line information from our new DynamoDB table
            List<Station> enrichedStations = stationsFromOverpass.stream()
                .map(station -> {
                    Optional<String> lineName = stationDataService.getLineForStation(station.name(), station.lat(), station.lon());
                    return new Station(station.id(), station.name(), station.lat(), station.lon(), lineName.orElse(null), station.distance());
                })
                .collect(Collectors.toList());

            // 3. Sort enriched stations by distance and then de-duplicate by name and line
            List<Station> allEnrichedStationsSorted = enrichedStations.stream()
                .sorted(Comparator.comparingDouble(Station::distance))
                .collect(Collectors.toList());

            List<Station> finalStations = new ArrayList<>();
            Set<String> uniqueStationKeys = new HashSet<>(); // To track unique "name::line" combinations

            for (Station station : allEnrichedStationsSorted) {
                // Construct a unique key for the station (name + line)
                // If line is null, append "::NULL" to the key to treat it as unique
                String stationKey = station.name() + "::" + (station.line() != null ? station.line() : "NULL");

                if (uniqueStationKeys.add(stationKey)) {
                    finalStations.add(station);
                    if (finalStations.size() >= limit) {
                        break; // We have enough unique stations
                    }
                }
            }
            
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("response", Map.of("station", finalStations));
            return OBJECT_MAPPER.writeValueAsString(responseMap);

        } catch (Exception e) {
            context.getLogger().log("[FATAL] Unhandled exception: " + e.toString());
            e.printStackTrace(); // For more detailed logs in CloudWatch
            return createErrorResponse("An unexpected error occurred. Check Lambda logs for details.");
        }
    }

    private List<Station> findStationsViaOverpass(double lat, double lng, int limit, Context context) {
        double currentRadius;
        if (limit <= 3) {
            currentRadius = 500.0;
        } else if (limit <= 5) {
            currentRadius = 1000.0;
        } else {
            currentRadius = 2000.0;
        }
        
        Set<Long> processedStationIds = new HashSet<>();
        List<Station> foundStations = new ArrayList<>();
        final int targetStationCount = limit * 2;

        while (foundStations.size() < targetStationCount && currentRadius <= 64000.0) {
            String query = String.format("[out:json];node(around:%f,%f,%f)[railway=station];out body;", currentRadius, lat, lng);
            context.getLogger().log("Overpass Query URL: https://overpass-api.de/api/interpreter with body: " + query);
            
            int maxRetries = 3;
            int attempt = 0;
            HttpResponse<String> response = null;

            while (attempt < maxRetries) {
                attempt++;
                try {
                    HttpRequest request = HttpRequest.newBuilder().uri(URI.create("https://overpass-api.de/api/interpreter")).POST(HttpRequest.BodyPublishers.ofString(query)).build();
                    response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() < 500) {
                        break;
                    }
                    
                    context.getLogger().log(String.format("[WARN] Overpass API attempt %d failed with status %d. Retrying in 1 second...", attempt, response.statusCode()));
                    Thread.sleep(1000);

                } catch (Exception e) {
                    context.getLogger().log(String.format("[WARN] Overpass API call failed at radius %f on attempt %d: %s", currentRadius, attempt, e.getMessage()));
                    response = null;
                    if (attempt >= maxRetries) break;
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            try {
                if (response == null || response.statusCode() != 200) { 
                    if (response != null) {
                        context.getLogger().log("Overpass API returned status code: " + response.statusCode() + " after " + attempt + " attempts.");
                    } else {
                        context.getLogger().log("Overpass API calls failed after " + attempt + " attempts.");
                    }
                    break; 
                }

                JsonNode root = OBJECT_MAPPER.readTree(response.body());
                int elementsFound = root.path("elements").size();
                context.getLogger().log("Overpass API returned " + elementsFound + " elements.");
                
                int stationsBeforeQuery = foundStations.size();
                for (JsonNode element : root.path("elements")) {
                    long id = element.get("id").asLong();
                    if (processedStationIds.add(id)) {
                        JsonNode tags = element.path("tags");
                        if (tags.has("name")) {
                            double stationLat = element.get("lat").asDouble();
                            double stationLon = element.get("lon").asDouble();
                            foundStations.add(new Station(id, tags.get("name").asText(), stationLat, stationLon, null, haversineDistance(lat, lng, stationLat, stationLon)));
                        }
                    }
                }
                
                // Adaptive Radius Expansion Logic
                if (foundStations.size() == stationsBeforeQuery) { // No new stations found
                     currentRadius *= 8;
                     context.getLogger().log("No new stations found. Expanding radius by 8x to " + currentRadius + "m");
                } else if (foundStations.size() < (targetStationCount / 4)) {
                    currentRadius *= 4;
                    context.getLogger().log("Found stations are less than 1/4 of target. Expanding radius by 4x to " + currentRadius + "m");
                } else {
                    currentRadius *= 2;
                }

            } catch (Exception e) {
                context.getLogger().log("[WARN] Overpass API processing failed at radius " + currentRadius + ": " + e.getMessage());
                break;
            }
        }
        return foundStations;
    }

    // New service class to query our pre-populated DynamoDB table
    private class StationDataService {
        private static final String TABLE_NAME = "station_data_v2";
        private static final String GSI_NAME = "station_name-index";
        private final DynamoDbClient dynamoDbClient;
        private final Context context;

        public StationDataService(Context context) {
            this.context = context;
            this.dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
        }

        public Optional<String> getLineForStation(String stationName, double lat, double lon) {
            context.getLogger().log("[INFO] Querying DynamoDB for station: " + stationName + " at lat: " + lat + ", lon: " + lon);
            try {
                // Query DynamoDB using the GSI on station_name
                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(":stationNameVal", AttributeValue.builder().s(stationName).build());

                QueryRequest queryRequest = QueryRequest.builder()
                        .tableName(TABLE_NAME)
                        .indexName(GSI_NAME)
                        .keyConditionExpression("station_name = :stationNameVal")
                        .expressionAttributeValues(expressionAttributeValues)
                        .build();

                QueryResponse response = dynamoDbClient.query(queryRequest);

                if (response.items() == null || response.items().isEmpty()) {
                    context.getLogger().log("[INFO] No match found in DynamoDB for station: " + stationName);
                    return Optional.empty();
                }

                Optional<String> lineName = response.items().stream()
                        .min(Comparator.comparingDouble(item -> 
                            haversineDistance(lat, lon, 
                                Double.parseDouble(item.get("lat").n()), 
                                Double.parseDouble(item.get("lon").n()))
                        ))
                        .map(item -> item.get("line_name").s());
                
                if (lineName.isPresent()) {
                    context.getLogger().log("[INFO] Found line '" + lineName.get() + "' for station: " + stationName);
                } else {
                    context.getLogger().log("[INFO] No line found in DynamoDB for station: " + stationName + " despite matching entries.");
                }
                return lineName;

            } catch (Exception e) {
                context.getLogger().log(String.format("[ERROR] Failed to query DynamoDB for station %s: %s", stationName, e.getMessage()));
                e.printStackTrace();
                return Optional.empty();
            }
        }
    }
    
    private double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0; // Radius of Earth in kilometers
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                 * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c * 1000; // convert to meters
    }

    private String createErrorResponse(String message) {
        try {
            return OBJECT_MAPPER.writeValueAsString(Map.of("error", message));
        } catch (Exception e) {
            // This should not happen
            return "{\"error\": \"Failed to create error response.\"}";
        }
    }
}