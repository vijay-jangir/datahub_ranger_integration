package com.vijayjangir.ranger.datahub_ranger_tagsync.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.*;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatahubEntity {
    private String urn;
    private String platform;
    private String name;
    private List<String> tableTags;
    private Map<String, List<String>> fieldTagMap;

    @JsonProperty("datasetKey")
    private void unpackDatasetKey(Map<String, Map<String, String>> fields) {
        this.platform = fields.get("value").get("platform");
        this.name = fields.get("value").get("name");
    }

    @JsonProperty("editableSchemaMetadata")
    private void unpackEditableSchemaMetadata(Map<String, Map<String, Object>> schemaMetadata) {
        // unpack editableSchemaMetadata to top level

        Map<String, Object> value = schemaMetadata.get("value");
        List<Map<String, Object>> editableSchemaFieldInfo = (List<Map<String, Object>>) value.get("editableSchemaFieldInfo");
        for (Map<String, Object> fieldInfo : editableSchemaFieldInfo) {
            if (fieldInfo.get("globalTags") != null && ((Map<String, Object>) fieldInfo.get("globalTags")).get("tags") != null) {
                String fieldPath = (String) fieldInfo.get("fieldPath");
                List<Map<String, String>> tags = (List<Map<String, String>>) ((Map<String, Object>) fieldInfo.get("globalTags")).get("tags");
                List<String> tagList = new ArrayList<>();
                for (Map<String, String> tag : tags) {
                    tagList.add(tag.get("tag"));
                }
                if (fieldTagMap == null) {
                    fieldTagMap = new HashMap<>();
                }
                fieldTagMap.put(fieldPath, tagList);
            }
        }
    }

    @JsonProperty("schemaMetadata")
    private void unpackSchemaMetadata(Map<String, Map<String, Object>> schemaMetadata) {
        // unpack editableSchemaMetadata to top level

        Map<String, Object> value = schemaMetadata.get("value");
        List<Map<String, Object>> fields = (List<Map<String, Object>>) value.get("fields");
        for (Map<String, Object> field : fields) {
            if (field.get("globalTags") != null && ((Map<String, Object>) field.get("globalTags")).get("tags") != null) {
                String fieldPath = (String) field.get("fieldPath");
                List<Map<String, String>> tags = (List<Map<String, String>>) ((Map<String, Object>) field.get("globalTags")).get("tags");
                List<String> tagList = new ArrayList<>();
                for (Map<String, String> tag : tags) {
                    tagList.add(tag.get("tag"));
                }
                if (fieldTagMap == null) {
                    fieldTagMap = new HashMap<>();
                }
                fieldTagMap.put(fieldPath, tagList);
            }
        }
    }

    @JsonProperty("globalTags")
    private void unpackGlobalTags(Map<String, Map<String, Object>> globalTags) {
        // unpack globalTags to top level
        List<Map<String, String>> tags = (List<Map<String, String>>) globalTags.get("value").get("tags");
        if (!tags.isEmpty() && tableTags == null) {
            tableTags = new ArrayList<>();
        }
        for (Map<String, String> tag : tags) {
            tableTags.add(tag.get("tag"));
        }
    }

    public Set<String> getAllTags() {
        Set<String> combinedTags = new HashSet<>(); // Initialize with table tags

        if (tableTags != null)
            combinedTags.addAll(tableTags);

        // Add all tags from map values
        if (fieldTagMap != null)
            for (List<String> tags : fieldTagMap.values()) {
                combinedTags.addAll(tags);
            }

        return combinedTags;
    }

}
