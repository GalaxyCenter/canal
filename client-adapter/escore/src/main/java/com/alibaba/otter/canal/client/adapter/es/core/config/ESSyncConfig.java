package com.alibaba.otter.canal.client.adapter.es.core.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import org.springframework.beans.factory.annotation.Value;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig implements AdapterConfig {

    private String    dataSourceKey;    // 数据源key

    private String    outerAdapterKey;  // adapter key

    private String    groupId;          // group id

    private String    destination;      // canal destination

    private ESMapping esMapping;

    private String    esVersion = "es6";

    public void validate() {
        if (esMapping.index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if ("es6".equals(esVersion) && esMapping.type == null) {
            throw new NullPointerException("esMapping._type");
        }
        if (esMapping.id == null && esMapping.getPk() == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }
        if (esMapping.sql == null) {
            throw new NullPointerException("esMapping.sql");
        }
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    @Override
    public String getTableName() {
        return esMapping.index;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public ESMapping getEsMapping() {
        return esMapping;
    }

    public void setEsMapping(ESMapping esMapping) {
        this.esMapping = esMapping;
    }

    public ESMapping getMapping() {
        return esMapping;
    }

    public String getEsVersion() {
        return esVersion;
    }

    public void setEsVersion(String esVersion) {
        this.esVersion = esVersion;
    }



    public static class ESMapping implements AdapterMapping {

        @Value("${_index}")
        private String index;

        @Value("${_type}")
        private String type;

        @Value("${_id}")
        private String id;
        private boolean                      upsert          = false;
        private String                       pk;
        private Map<String, RelationMapping> relations       = new LinkedHashMap<>();
        private String                       sql;
        // 对象字段, 例: objFields:
        // - _labels: array:;
        private Map<String, String>          objFields       = new LinkedHashMap<>();
        private List<String>                 skips           = new ArrayList<>();
        private int                          commitBatch     = 1000;
        private String                       etlCondition;
        private boolean                      syncByTimestamp = false;                // 是否按时间戳定时同步
        private Long                         syncInterval;                           // 同步时间间隔

        private SchemaItem                   schemaItem;                             // sql解析结果模型

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean isUpsert() {
            return upsert;
        }

        public void setUpsert(boolean upsert) {
            this.upsert = upsert;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public Map<String, String> getObjFields() {
            return objFields;
        }

        public void setObjFields(Map<String, String> objFields) {
            this.objFields = objFields;
        }

        public List<String> getSkips() {
            return skips;
        }

        public void setSkips(List<String> skips) {
            this.skips = skips;
        }

        public Map<String, RelationMapping> getRelations() {
            return relations;
        }

        public void setRelations(Map<String, RelationMapping> relations) {
            this.relations = relations;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Long getSyncInterval() {
            return syncInterval;
        }

        public void setSyncInterval(Long syncInterval) {
            this.syncInterval = syncInterval;
        }

        public boolean isSyncByTimestamp() {
            return syncByTimestamp;
        }

        public void setSyncByTimestamp(boolean syncByTimestamp) {
            this.syncByTimestamp = syncByTimestamp;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }
    }

    public static class RelationMapping {

        private String name;
        private String parent;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }
    }
}
