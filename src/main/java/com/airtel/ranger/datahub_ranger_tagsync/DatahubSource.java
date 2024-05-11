package com.vijayjangir.ranger.datahub_ranger_tagsync;

import com.vijayjangir.ranger.datahub_ranger_tagsync.model.DatahubResponse;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class DatahubSource {
    private static final ObjectMapper mapper = new JsonMapper().builder().enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION).build();

    DatahubSource() {
    }

    /**
     * Pull dataset entries from Datahub which has tags at table level or field(column) level
     * @return DatahubResponse
     */
    public DatahubResponse getDatasetDetails() {
        DatahubResponse datasetResponse = new DatahubResponse();
        String endpointWithPagination = Constants.DATASET_ENDPOINT_FOR_TAGS;
        HttpClient client = HttpClient.newHttpClient();
        String scrollId = "";

        // keep iterating unless more pages are avaialble
        do {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpointWithPagination))
                    .header("Authorization", "Bearer " + Constants.DATAHUB_TOKEN)
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    DatahubResponse dataset = mapper.readValue(response.body(), DatahubResponse.class);
                    if (datasetResponse.getEntities() != null) {
                        datasetResponse.getEntities().addAll(dataset.getEntities());
                    } else {
                        datasetResponse.setEntities(dataset.getEntities());
                    }
                    scrollId = dataset.getScrollId();
                    endpointWithPagination = Constants.DATASET_ENDPOINT_FOR_TAGS + "&scrollId=" + scrollId;
                } else {
                    throw new IOException("Failed with response code: " + response.statusCode());
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to get dataset details: " + e.getMessage() + ". Request sent: " + request, e);
            }
        } while (scrollId != null);

        return datasetResponse;
    }
}
