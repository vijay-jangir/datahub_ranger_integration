package com.vijayjangir.ranger.datahub_ranger_tagsync;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Constants {
    public static final String DATAHUB_ENDPOINT = "/openapi/v2/entity/dataset";
    public static final String RANGER_TAG_PUSH_ENDPOINT = "/service/tags/importservicetags";
    private static final Config config = Config.getInstance();// public static final String DATAHUB_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6IkIwMjI4MTg4IiwidHlwZSI6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiIwYWYyMjFlYy1jZGFlLTQ4MDItYTliNy04Yzg3NTRmODgwNGYiLCJzdWIiOiJCMDIyODE4OCIsImV4cCI6MTcxNTc3ODc5NCwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.C6rPYe7gSh2wHlJM08GLlF3OGZsf2CY_-XuSQKGK7Qw";
    // public static final String DATAHUB_URL = "https://didatahub.vijayjangir.com";
    public static final String DATAHUB_TOKEN = Config.getDatahubToken();
    public static final String DATAHUB_URL = Config.getDatahubUrl();
    public static final String POLL_BATCH_COUNT = Config.getDatahubPollBatchCount();
    public static final String DATAHUB_STRUCTURED_QUERY = Config.getDatahubQuery() != null ? URLEncoder.encode(Config.getDatahubQuery(), StandardCharsets.UTF_8) : "%2Aurn%5C%3Ali%5C%3AdataPlatform%5C%3Atrino%2A";
    public static final String DATASET_ENDPOINT_FOR_TAGS = DATAHUB_URL + DATAHUB_ENDPOINT + "?systemMetadata=false&aspects=editableSchemaMetadata,globalTags,schemaMetadata&count=" + POLL_BATCH_COUNT + "&query=" + DATAHUB_STRUCTURED_QUERY;
    public static final String RANGER_ADMIN_HOST = Config.getRangerUrl();
    public static final String RANGER_TAG_PUSH_URL = RANGER_ADMIN_HOST + RANGER_TAG_PUSH_ENDPOINT;
    public static final String RANGER_ADMIN_USER = Config.getRangerUser();
    public static final String RANGER_ADMIN_PASSWORD = Config.getRangerPassword();
    public static final Map<String, String> DATAHUB_TO_RANGER_SERVICE_MAP = Config.getDatahubRangerServiceMapping();

}