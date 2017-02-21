// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
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
    // TODO: move queries to properties file
    HsqldbEdgeDao(DataSource dataSource, RdbmsGraph graph) {
        ds = dataSource;
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
    }
    // =================================
    private ResultSetHandler<RdbmsEdge> makeEdge = new ResultSetHandler<RdbmsEdge>() {
        @Override
        public RdbmsEdge handle(ResultSet rs) throws SQLException {
            return new RdbmsEdge(rs.getLong(1),
                    rs.getLong(2),
                    rs.getLong(3),
                    rs.getString(4),
                    graph);
        }
    };
    // =================================
    // TODO
    @Override
    public RdbmsEdge add(long outVertexID, long inVertexID, final String label) {
        String sql = "insert into edge (out_vertex_id, in_vertex_id, label) values (:outID, :inID, :label)";

        try (Connection con = sql2o.open()) {
            Long genID = con.createQuery(sql, "add edge", true)
                               .addParameter("outID", outVertexID)
                               .addParameter("inID", inVertexID)
                               .addParameter("label", label)
                               .executeUpdate()
                               .getKey(Long.class);
            log.info("generated edge id returned {}", genID);
            return new RdbmsEdge(genID.longValue(), outVertexID, inVertexID, label, graph);
        }
    }
    // =================================
    @Override
    public RdbmsEdge get(long id) {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // TODO: list columns explicitly
        // edge in turn, it adds another round-trip to the DB
        String sql = "select * from edge where id = :id";

        try (Connection con = sql2o.open()) {
            RdbmsEdge e = con.createQuery(sql, "get edge "+id)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeEdge);
            log.info("returned edge for {}: {}", id, e);
            return e;
        }
    }
    // =================================
    @Override
    public void remove(long id) {
        String sql0 = "delete from edge where id = :id";
        String sql1 = "delete from property where element_id = :id";

        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("remove edge w id {}", id);
            con.createQuery(sql0, "remove edge "+id) .addParameter("id", id) .executeUpdate();
            con.createQuery(sql1, "remove properties for "+id) .addParameter("id", id) .executeUpdate();
        }
    }
    // =================================
    @Override
    public Iterable<RdbmsEdge> list() {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select * from edge";

        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("request all edges");
            return con.createQuery(sql, "get all edges")
                      .executeAndFetch(makeEdge);
        }
    }
    // =================================
    @Override
    public Iterable<RdbmsEdge> list(String key, Object value) {
        return Collections.emptyList();
    }
    // =================================
    private final static String sqlEdgeByVertex = "select * from edge where out_vertex_id = :id or in_vertex_id = :id";
    @Override
    public Iterable<RdbmsEdge> list(Long vertexId) {
        try (Connection con = sql2o.open()) {
            return con.createQuery(sqlEdgeByVertex, "get edges by vertex "+vertexId)
                    .addParameter("id", vertexId)
                    .executeAndFetch(makeEdge);
        }
    }
    // =================================
    static String sqlClear = "truncate table edge restart identity and commit no check";
    @Override
    public void clear() {
        try (Connection con = sql2o.open()) {
            con.createQuery(sqlClear, "clear edges").executeUpdate();
        }
    }
    // =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
