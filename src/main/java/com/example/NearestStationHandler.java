package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class NearestStationHandler implements RequestHandler<Map<String, Object>, String> {

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        @SuppressWarnings("unchecked")
        Map<String, String> params = (Map<String, String>) event.get("queryStringParameters");

        if (params == null || !params.containsKey("lat") || !params.containsKey("lng")) {
            return "{\"error\": \"Please specify lat and lng.\"}";
        }

        String lat = params.get("lat");
        String lng = params.get("lng");

        String url = String.format(
            "http://express.heartrails.com/api/json?method=getStations&x=%s&y=%s",
            lng, lat
        );

        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return "{\"error\": \"API Connection failed\"}";
        }
    }
}