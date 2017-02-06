// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.util.Properties;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.impls.rdbms.ConnectionPoolManager;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.SchemaVersionManager;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.KryoSerializer;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbDaoFactory implements DaoFactory {

    // TODO: do we need a thread-local kryo?

    public static HsqldbDaoFactory make(Properties hikariConfig, RdbmsGraph graph) {
        return new HsqldbDaoFactory(hikariConfig, graph);
    }
    // =================================
    private HsqldbDaoFactory(Properties hikariConfig_, RdbmsGraph graph_) {
        cpm = new ConnectionPoolManager(hikariConfig_);
        ds = cpm.getDataSource();
        graph = graph_;

        log.info("CPM: {}", cpm);

        new SchemaVersionManager(cpm.getDataSource()).migrate();

        SerializeDao sd = new HsqldbSerializerDao(ds);
        serializer = new KryoSerializer(sd);
    }
    // =================================
    @Override
    public VertexDao getVertexDao() {
        return new HsqldbVertexDao(ds, graph);
    }
    // =================================
    @Override
    public EdgeDao getEdgeDao() {
    	return new HsqldbEdgeDao(ds, graph);
    }
    // =================================
    @Override
    public PropertyDao getPropertyDao() {
    	return new HsqldbPropertyDao(ds, graph, serializer);
    }
    // =================================
    private final ConnectionPoolManager cpm;
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Serializer serializer;

}
