/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class TDengineDatabaseAccessor implements DatabaseAccessor {

  protected static final String DBCP_CONFIG_PREFIX = JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final Logger LOGGER = LoggerFactory.getLogger(TDengineDatabaseAccessor.class);
  protected DataSource dbcpDataSource = null;

  private String str = ".*\\s+from\\s+(.*)";
  private Pattern pattern = Pattern.compile(str);


  public TDengineDatabaseAccessor() {
  }


  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String metadataQuery = addLimitToQuery(sql, 1);

      conn = dbcpDataSource.getConnection();

      StringBuffer sb = new StringBuffer(metadataQuery);
      Matcher matcher = pattern.matcher(metadataQuery);
      if(matcher.find()){
        int tableIndex = matcher.start(1);
        //在表名前加上数据库名
        sb.insert(tableIndex, conn.getCatalog() + ".");
      }

      metadataQuery = sb.toString();
      LOGGER.info("getColumnNames Query to execute is [{}]", metadataQuery);

      Statement stmt = conn.createStatement();
      rs = stmt.executeQuery(metadataQuery);

      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      List<String> columnNames = new ArrayList<String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columnNames.add(metadata.getColumnName(i + 1));
      }

      return columnNames;
    }
    catch (Exception e) {
      LOGGER.error("Error while trying to get column names.", e);
      throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }

  }

  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);

      conn = dbcpDataSource.getConnection();

      Matcher matcher = pattern.matcher(sql);
      String tableName = "";
      if(matcher.find()){
        tableName = matcher.group(1);
      }

      String countQuery = "SELECT COUNT(*) FROM " + conn.getCatalog() + "." + tableName;
      LOGGER.debug("Query to execute is [{}]", countQuery);

      Statement stmt = conn.createStatement();
      rs = stmt.executeQuery(countQuery);
      if (rs.next()) {
        return rs.getInt(1);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        return 0;
//        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records", e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }


  @Override
  public JdbcRecordIterator
    getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String limitQuery = addLimitAndOffsetToQuery(sql, limit, offset);

      conn = dbcpDataSource.getConnection();

      StringBuffer sb = new StringBuffer(limitQuery);
      Matcher matcher = pattern.matcher(limitQuery);
      if(matcher.find()){
        int tableIndex = matcher.start(1);
        sb.insert(tableIndex, conn.getCatalog() + ".");
      }

      limitQuery = sb.toString();
      LOGGER.info("Query to execute is [{}]", limitQuery);

      Statement stmt = conn.createStatement();
//      stmt.executeUpdate("use " +  conn.getCatalog());
      rs = stmt.executeQuery(limitQuery);
      LOGGER.info("ResultSet rowcount is [{}]", rs.getFetchSize());
      return new JdbcRecordIterator(conn, ps, rs);
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
  }


  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   *
   * @param sql
   * @param limit
   * @param offset
   * @return
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    }
    else {
      return sql + " LIMIT " + limit + " OFFSET " + offset + "";
    }
  }


  /*
   * Uses generic JDBC escape functions to add a limit clause to a query string
   */
  protected String addLimitToQuery(String sql, int limit) {
    return sql + " LIMIT " + limit + ";";
  }


  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during resultset cleanup.", e);
    }

    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during statement cleanup.", e);
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during connection cleanup.", e);
    }
  }

  public void initializeDatabaseConnection(Configuration conf) throws Exception {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
        }
      }
    }
  }


  protected Properties getConnectionPoolProperties(Configuration conf) {
    // Create the default properties object
    Properties dbProperties = getDefaultDBCPProperties();

    // override with user defined properties
    Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
    if ((userProperties != null) && (!userProperties.isEmpty())) {
      for (Entry<String, String> entry : userProperties.entrySet()) {
        dbProperties.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
      }
    }

    // essential properties that shouldn't be overridden by users
    dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
    dbProperties.put("driverClassName", conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
    dbProperties.put("type", "javax.sql.DataSource");
    return dbProperties;
  }


  protected Properties getDefaultDBCPProperties() {
    Properties props = new Properties();
    props.put("initialSize", "1");
    props.put("maxActive", "3");
    props.put("maxIdle", "0");
    props.put("maxWait", "10000");
    props.put("timeBetweenEvictionRunsMillis", "30000");
    return props;
  }


  protected int getFetchSize(Configuration conf) {
    return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }
}
