package com.tinkerpop.blueprints.impls.rdbms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Map.Entry;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@ToString
public class ConnectionPoolManager implements AutoCloseable {
    // =================================
    public ConnectionPoolManager(final Properties properties) {
        if (log.isInfoEnabled()) {
            log.info("Properties...");
            for (Entry<Object, Object> e : properties.entrySet()) {
                log.info("{} => {}", e.getKey(), e.getValue());
            }
        }
        ds_ = new HikariDataSource(new HikariConfig(properties));
    }
    // =================================
    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager#getConnection()
     */
    public Connection getConnection() throws SQLException {
        log.trace("getConnection {}", ds_);
        return ds_.getConnection();
    }
    // =================================
    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager#getDataSource()
     */
    public DataSource getDataSource() {
        log.trace("getDataSource {}", ds_);
        return ds_;
    }
    // =================================
    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager#close()
     */
    @Override
    public void close() throws SQLException {
        log.trace("ConnectionPoolManager.close");
        shutdown();
    }
    // =================================
    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager#shutdown()
     */
    public void shutdown() throws SQLException {
        log.trace("ConnectionPoolManager.shutdown");
        ds_.close();
    }
    // =================================
    private final HikariDataSource ds_;

}
