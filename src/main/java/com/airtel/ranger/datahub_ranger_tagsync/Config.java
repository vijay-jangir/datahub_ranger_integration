package com.vijayjangir.ranger.datahub_ranger_tagsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    private static Config instance;
    private static Map<String, Object> config;

    private Config() {
        Yaml yaml = new Yaml();
        String configLocation = System.getProperty("config.location");
        if (configLocation == null) {
            logger.error("config.location system property not set. set it using -Dconfig.location=<path to config file>");
            throw new RuntimeException("config.location system property not set");
        }
        try (var in = new FileInputStream(configLocation)) {
            config = yaml.load(in);
        } catch (IOException e) {
            logger.error("Failed to load config from " + configLocation, e);
            throw new RuntimeException("Failed to load config from " + configLocation, e);
        }
    }

    public static Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public static String getDatahubToken() {
        return (String) ((Map) config.get("datahub")).get("token");
    }

    public static String getDatahubUrl() {
        return (String) ((Map) config.get("datahub")).get("url");
    }

    public static String getDatahubPollBatchCount() {
        return (String) ((Map) config.get("datahub")).get("pollBatchCount");
    }

    public static String getDatahubQuery() {
        return (String) ((Map) config.get("datahub")).get("query");
    }

    public static String getRangerUrl() {
        return (String) ((Map) config.get("ranger")).get("url");
    }

    public static String getRangerUser() {
        return (String) ((Map) config.get("ranger")).get("user");
    }

    public static String getRangerPassword() {
        return (String) ((Map) config.get("ranger")).get("password");
    }

    public static Map<String, String> getDatahubRangerServiceMapping() {
        return (Map<String, String>) config.get("datahubRangerServiceMapping");
    }
}