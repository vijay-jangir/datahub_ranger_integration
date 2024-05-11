package com.vijayjangir.ranger.datahub_ranger_tagsync;

import com.vijayjangir.ranger.datahub_ranger_tagsync.model.DatahubEntity;
import com.vijayjangir.ranger.datahub_ranger_tagsync.model.DatahubResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RangerTagSync {
    private static final Logger logger = LoggerFactory.getLogger(RangerTagSync.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static long startTime = System.currentTimeMillis();

    public static void main(String[] args) throws JsonProcessingException {
        logger.info("Started Datahub to Ranger tagsync service");
        DatahubSource datahubSource = new DatahubSource();
        DatahubResponse datasets = datahubSource.getDatasetDetails();

        logger.info("Time taken to fetch datahub datasets: {} ms", System.currentTimeMillis() - startTime);
        logger.info("Number of datahub datasets fetched: {}", datasets.getEntities().size());
        startTime = System.currentTimeMillis();

        // convert DatahubDataset to RangerEntity
        List<ServiceTags> tableResources = new ArrayList<>();
        for (DatahubEntity dataset : datasets.getEntities()) {
            // convert DatahubDataset to RangerEntity
            ServiceTags serviceTags = Utils.convertToRangerEntity(dataset);
            tableResources.add(serviceTags);
        }

        logger.info("Time taken to convert datahub datasets to ranger entities: {} ms", System.currentTimeMillis() - startTime);
        logger.info("Number of Ranger entities prepared: {}", tableResources.size());
        startTime = System.currentTimeMillis();

        RangerSink rangerSink = new RangerSink();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        logger.info("number of table resources: {}", tableResources.size());

        try {
            for (ServiceTags tableResource : tableResources) {
                JsonNode node = mapper.valueToTree(tableResource);
                Utils.removeNullNodes(node);
                String jsonWithoutNulls = mapper.writeValueAsString(node);

                futures.add(rangerSink.pushTags(jsonWithoutNulls).thenAccept(response -> {
                    long requestStartTime = System.currentTimeMillis();
                    if (response.statusCode() >= 400) {
                        logger.error("Failed to push tags for {} with response code: {}", tableResource.getServiceResources(), response.statusCode());
                        throw new RuntimeException("Failed to push tags for " + tableResource + " with response code: " + response.statusCode());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("Successfully pushed tags for {} with response code: {}. Duration: {} ms", tableResource, response.statusCode(), (System.currentTimeMillis() - requestStartTime));
                    }
                }));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            logger.info("Time taken to put tags into ranger: {} ms", System.currentTimeMillis() - startTime);
        } finally {
            rangerSink.close();
        }
    }


}
