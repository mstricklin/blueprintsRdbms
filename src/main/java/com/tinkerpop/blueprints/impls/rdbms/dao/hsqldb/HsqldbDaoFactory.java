// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.SchemaVersionManager;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.KryoSerializer;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

@Slf4j
public class HsqldbDaoFactory implements DaoFactory {

    public static HsqldbDaoFactory make(Properties hikariConfig, RdbmsGraph graph) {
        return new HsqldbDaoFactory(hikariConfig, graph);
    }
    // =================================
    private HsqldbDaoFactory(Properties hikariConfig_, RdbmsGraph graph_) {
        cpm = new ConnectionPoolManager(hikariConfig_);
        graph = graph_;

        log.info("HsqldbDaoFactory CPM: {}", cpm);

        new SchemaVersionManager(cpm.getDataSource()).migrate();
        log.info("Done migrate");

        ds = cpm.getDataSource();
        SerializerDao sd = new HsqldbKryoDao(ds);
        serializer = new KryoSerializer(sd);
    }
    // =================================
    public void clear() {
        log.warn("clearing database");
        getVertexDao().clear();
        getEdgeDao().clear();
        HsqldbPropertyDao.clear(ds);
    }
    // =================================
    @Override
    public void close() {
        try {
            if (null != cpm) {
                log.info("Shutting down connection...");
                cpm.close();
            }
        } catch (SQLException e) {
            log.error("Error shutting down DB", e);
        }
    }
    // =================================
    @Override
    public VertexDao getVertexDao() {
        return new HsqldbVertexDao(ds, graph, serializer);
    }
    // =================================
    @Override
    public EdgeDao getEdgeDao() {
    	return new HsqldbEdgeDao(ds, graph, serializer);
    }
    // =================================
    @Override
    public PropertyDao getPropertyDao(RdbmsElement.PropertyType type) {
        return new HsqldbPropertyDao(type, ds, serializer);
    }
    // =================================
    private final ConnectionPoolManager cpm;
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Serializer serializer;

}
