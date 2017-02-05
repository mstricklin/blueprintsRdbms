// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.util.Properties;

import javax.sql.DataSource;


import com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.SchemaVersionManager;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.EdgeDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.VertexDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbDaoFactory implements DaoFactory {

    public static HsqldbDaoFactory make(Properties hikariConfig, RdbmsGraph graph) {
        return new HsqldbDaoFactory(hikariConfig, graph);
    }
    // =================================
    private HsqldbDaoFactory(Properties hikariConfig, RdbmsGraph graph) {
        cpm_ = new ConnectionPoolManager(hikariConfig);
        ds_ = cpm_.getDataSource();
        graph_ = graph;

        log.info("CPM: {}", cpm_);

        new SchemaVersionManager(cpm_.getDataSource()).migrate();

        // TODO: get queries?
    }
    // =================================
    @Override
    public VertexDao getVertexDao() {
        return new HsqldbVertexDao(ds_, graph_);
    }
    // =================================
    @Override
    public EdgeDao getEdgeDao() {
    	return new HsqldbEdgeDao(ds_, graph_);
    }
    // =================================
    @Override
    public PropertyDao getPropertyDao() {
    	return new HsqldbPropertyDao(ds_, graph_);
    }
    // =================================

    private final ConnectionPoolManager cpm_;
    private final DataSource ds_;
    private final RdbmsGraph graph_;
}
