// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.EdgeDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbEdgeDao implements EdgeDao {

    HsqldbEdgeDao(DataSource dataSource, RdbmsGraph graph) {
        ds = dataSource;
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
    }
    // =================================
    private ResultSetHandler<RdbmsEdge> makeEdge = new ResultSetHandler<RdbmsEdge>() {
        @Override
        public RdbmsEdge handle(ResultSet rs) throws SQLException {
            // TODO: think about coherence here...
            Vertex in = new RdbmsVertex(rs.getInt(2), graph);
            Vertex out = new RdbmsVertex(rs.getInt(3), graph);
            String label = rs.getString(4);
            return new RdbmsEdge(rs.getInt(1), out, in, label, graph);
        }
    };
    // =================================
    // TODO
    @Override
    public RdbmsEdge add(Vertex outVertex, Vertex inVertex, String label) {
        String sql = "insert into edge values (null)";

        try (Connection con = sql2o.open()) {
            Integer genID = con.createQuery(sql, true)
                               .executeUpdate()
                               .getKey(Integer.class);
            log.info("generated edge id returned {}", genID);
            return new RdbmsEdge(genID.intValue(), outVertex, inVertex, label, graph);
        }
    }
    // =================================
    @Override
    public RdbmsEdge get(Object id) {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // edge in turn, it adds another round-trip to the DB
        String sql = "select * from edge where id = :id";

        try (Connection con = sql2o.open()) {
            RdbmsEdge e = con.createQuery(sql)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeEdge);
            log.info("returned edge for {}: {}", id, e);
            return e;
        }
    }
    // =================================
    @Override
    public void remove(Object id) {
        String sql0 = "delete from edge where id = :id";
        String sql1 = "delete from property where element_id = :id";

        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("remove edge w id {}", id);
            con.createQuery(sql0) .addParameter("id", id) .executeUpdate();
            con.createQuery(sql1) .addParameter("id", id) .executeUpdate();
        }
    }
    // =================================
    @Override
    public Iterable<? extends Edge> list() {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select id from edge";

        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("request all edges");
            return con.createQuery(sql)
                      .executeAndFetch(makeEdge);
        }
    }
    // =================================
    @Override
    public Iterable<? extends Edge> list(String key, Object value) {
        return Collections.emptyList();
    }
    // =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
