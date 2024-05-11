package com.vijayjangir.ranger.datahub_ranger_tagsync.model;

import lombok.Data;

import java.util.List;


@Data
public class DatahubResponse {
    private List<DatahubEntity> entities;
    private String scrollId;
}
