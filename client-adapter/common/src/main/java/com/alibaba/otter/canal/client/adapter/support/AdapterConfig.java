package com.alibaba.otter.canal.client.adapter.support;

public interface AdapterConfig {
    String getDataSourceKey();

    String getTableName();

    AdapterMapping getMapping();

    interface AdapterMapping {
        String getEtlCondition();
    }
}
