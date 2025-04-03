package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * RDB ETL 操作业务类
 *
 * @author rewerma @ 2018-11-7
 * @version 1.0.0
 */
public class RdbEtlService extends AbstractEtlService {

    private DataSource    targetDS;
    private MappingConfig config;

    public RdbEtlService(DataSource targetDS, MappingConfig config){
        super("RDB", config);
        this.targetDS = targetDS;
        this.config = config;
    }

    /**
     * 导入数据
     */
    public EtlResult importData(List<String> params) {
        DbMapping dbMapping = config.getDbMapping();
        // 获取源数据源，根据数据库类型拼装数据库名和表名
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        String sql = "SELECT * FROM " + SyncUtil.getSourceDbTableName(dbMapping, dataSource.getDbType());
        return importData(sql, params);
    }

    /**
     * 执行导入
     */
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {
        try {
            DbMapping dbMapping = (DbMapping) mapping;
            Map<String, String> columnsMap = new LinkedHashMap<>();
            Map<String, Integer> columnType = new LinkedHashMap<>();
            DruidDataSource dataSource = (DruidDataSource) srcDS;
            String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

            Util.sqlRS(targetDS,
                "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " LIMIT 1 ",
                rs -> {
                try {

                    ResultSetMetaData rsd = rs.getMetaData();
                    int columnCount = rsd.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                        columns.add(rsd.getColumnName(i));
                    }

                    columnsMap.putAll(SyncUtil.getColumnsMap(dbMapping, columns));
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            });

            int threadCount = Runtime.getRuntime().availableProcessors() * 4;
            ExecutorService executor = Util.newFixedThreadPool(threadCount, 30000L);
            List<Future> futures = new ArrayList<>();

            Util.sqlRS(srcDS, sql, values, rs -> {
                int idx = 1;
                int batchSize = 10000;
                List<Map<String, Object>> buffer = new ArrayList<>(batchSize);

                try {
                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("INSERT INTO ")
                        .append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()))
                        .append(" (");
                    columnsMap
                        .forEach((targetColumnName, srcColumnName) -> insertSql.append(backtick).append(targetColumnName).append(backtick).append(","));

                    int len = insertSql.length();
                    insertSql.delete(len - 1, len).append(") VALUES (");
                    int mapLen = columnsMap.size();
                    for (int i = 0; i < mapLen; i++) {
                        insertSql.append("?,");
                    }
                    len = insertSql.length();
                    insertSql.delete(len - 1, len).append(")");

                    while (rs.next()) {
                        Map<String, Object> rowValues = new HashMap();
                        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                            String targetColumnName = entry.getKey();
                            String srcColumnName = entry.getValue();
                            if (srcColumnName == null) {
                                srcColumnName = targetColumnName;
                            }

                            Object value = rs.getObject(srcColumnName);
                            rowValues.put(srcColumnName, value);
                        }
                        buffer.add(rowValues);

                        if (++idx % batchSize == 0) {
                            List<Map<String, Object>> rows = new ArrayList<>(buffer);
                            Future future = executor.submit(() -> doBatchInsert(rows, mapping, columnsMap, columnType, insertSql.toString()));
                            futures.add(future);

                            impCount.addAndGet(buffer.size());
                            buffer.clear();
//                            if (logger.isDebugEnabled()) {
//                                logger.debug("table:" + dbMapping.getTable() + "successful import count:" + impCount.get());
//                            }
                        }
                    }
                    // 处理剩余不足一批的数据
                    if (!buffer.isEmpty()) {
                        doBatchInsert(buffer, mapping, columnsMap, columnType, insertSql.toString());
                        impCount.addAndGet(buffer.size());
                    }

                    for (Future<Boolean> future : futures) {
                        future.get();
                    }
                    executor.shutdown();
                } catch (Exception e) {
                    logger.error(dbMapping.getTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(dbMapping.getTable() + " etl failed! ==>" + e.getMessage());
                }
                return idx;
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    protected void fillPreparedStatement(PreparedStatement preparedStatement, Map<String, Object> row, Map<String, String> columnsMap, Map<String, Integer> columnType)
            throws SQLException {
        int i = 1;
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }

            Integer type = columnType.get(targetColumnName.toLowerCase());
            Object value = row.get(srcColumnName);
            if (value != null) {
                SyncUtil.setPStmt(type, preparedStatement, value, i);
            } else {
                preparedStatement.setNull(i, type);
            }
            i++;
        }
    }

    private void doBatchInsert(List<Map<String, Object>> buffer,
                               AdapterConfig.AdapterMapping mapping,
                               Map<String, String> columnsMap, Map<String, Integer> columnType, String insertSql) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = targetDS.getConnection();
            pstmt = conn.prepareStatement(insertSql);
            conn.setAutoCommit(true);
            for (Map<String, Object> row : buffer) {
                fillPreparedStatement(pstmt, row, columnsMap, columnType);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            logger.info("回滚批量写入, 采用单条插入的方式写入数据. 因为:" + e.getMessage());
            try {
                conn.rollback();
                doOneInsert(buffer, mapping, conn, columnsMap, columnType, insertSql);
            } catch (SQLException e1) {
                logger.info("写入数据失败. 因为:" + e.getMessage());
                logger.error(e1.getMessage(), e1);
            }
        } finally {
            Util.closeDBResources(pstmt, conn);
        }
    }

    private void doOneInsert(List<Map<String, Object>> buffer,
                             AdapterConfig.AdapterMapping mapping,
                             Connection conn,
                             Map<String, String> columnsMap, Map<String, Integer> columnType,
                             String insertSql) throws SQLException {
        PreparedStatement pstmt = null;
        DruidDataSource dataSource = (DruidDataSource) targetDS;
        DbMapping dbMapping = (DbMapping) mapping;

        for (Map<String, Object> row : buffer) {
            try {
                pstmt = conn.prepareStatement(insertSql);
                conn.setAutoCommit(true);

                fillPreparedStatement(pstmt, row, columnsMap, columnType);
                pstmt.execute();
            } catch (SQLException e) {
                logger.info("插入数据失败, 将删除重新插入的方式. 因为:" + e.getMessage());
                String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
                try {
                    // 删除数据
                    Map<String, Object> pkVal = new LinkedHashMap<>();
                    StringBuilder deleteSql = new StringBuilder(
                            "DELETE FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " WHERE ");
                    appendCondition(dbMapping, deleteSql, pkVal, row, backtick);
                    try (PreparedStatement pstmt2 = conn.prepareStatement(deleteSql.toString())) {
                        int k = 1;
                        for (Object val : pkVal.values()) {
                            pstmt2.setObject(k++, val);
                        }
                        pstmt2.execute();
                    }
                    // 重新插入
                    pstmt.execute();
                } catch (SQLException e2) {
                    logger.error("插入数据失败, 因为:" + e.getMessage());
                }
            } finally {
                Util.closeDBResources(pstmt, null);
            }
        }
    }
    /**
     * 拼接目标表主键where条件
     */
    private static void appendCondition(DbMapping dbMapping, StringBuilder sql, Map<String, Object> values,
                                        Map<String, Object> row, String backtick) throws SQLException {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
            values.put(targetColumnName, row.get(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }
}
