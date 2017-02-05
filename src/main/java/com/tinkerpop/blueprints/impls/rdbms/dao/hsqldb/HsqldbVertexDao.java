// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.VertexDao;

import javassist.expr.NewArray;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbVertexDao implements VertexDao {

    HsqldbVertexDao(DataSource dataSource, RdbmsGraph graph) {
        ds = dataSource;
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
    }
    // =================================
    private ResultSetHandler<RdbmsVertex> makeVertex = new ResultSetHandler<RdbmsVertex>() {
        @Override
        public RdbmsVertex handle(ResultSet rs) throws SQLException {
            return new RdbmsVertex(rs.getInt(1), graph);
        }
    };
    // =================================
    @Override
    public RdbmsVertex add() {
        String sql = "insert into vertex values (null)";

        try (Connection con = sql2o.open()) {
            Integer genID = con.createQuery(sql, true)
                               .executeUpdate()
                               .getKey(Integer.class);
            log.info("generated vertex id returned {}", genID);
            return new RdbmsVertex(genID.intValue(), graph);
        }
    }
    // =================================
    @Override
    public RdbmsVertex get(Object id) {
        // TODO: this needs to be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select * from vertex where id = :id";

        try (Connection con = sql2o.open()) {
            RdbmsVertex v = con.createQuery(sql)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeVertex);
            log.info("returned vertex for {}: {}", id, v);
            return v;
        }
    }
    // =================================
    @Override
    public void remove(Object id) {

        String sql0 = "delete from vertex where id = :id";
        String sql1 = "delete from property where element_id = :id";

        try (Connection con = sql2o.open()) {
            log.info("remove vertex w id {}", id);
            con.createQuery(sql0) .addParameter("id", id) .executeUpdate();
            con.createQuery(sql1) .addParameter("id", id) .executeUpdate();
        }
    }
    // =================================
    @Override
    public Iterable<? extends Vertex> list() {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select id from vertex";

        try (Connection con = sql2o.open()) {
            return con.createQuery(sql).executeAndFetch(makeVertex);
        }
    }
    // =================================
    @Override
    public Iterable<? extends Vertex> list(String key, Object value) {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        // TODO: how to we serialize properties?
        String sql = "select distinct element_id from property where key = :key and value = :value";

        try (Connection con = sql2o.open()) {
            return con.createQuery(sql)
                    .addParameter("key", key)
                    .addParameter("value", value)
                    .addColumnMapping("element_id", "id")
                    .executeAndFetch(makeVertex);
        }
    }
    // =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
