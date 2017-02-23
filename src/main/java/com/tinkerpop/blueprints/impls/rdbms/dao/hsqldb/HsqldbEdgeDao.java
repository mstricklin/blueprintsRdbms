// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;
import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.EdgeDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbEdgeDao implements EdgeDao {
    // TODO: move queries to properties file
    HsqldbEdgeDao(DataSource dataSource, RdbmsGraph graph, Serializer serializer_) {
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
        serializer = serializer_;
    }
    // =================================
    private ResultSetHandler<RdbmsEdge> makeEdge = new ResultSetHandler<RdbmsEdge>() {
        @Override
        public RdbmsEdge handle(ResultSet rs) throws SQLException {
            return new RdbmsEdge(rs.getLong(1),  // edge ID
                    rs.getLong(2),               // out vertex ID
                    rs.getLong(3),               // in vertex ID
                    rs.getString(4),             // label
                    graph);
        }
    };
    // =================================
    private static final String ADD_QUERY =
            "insert into edge (out_vertex_id, in_vertex_id, label) " +
                    "values (:outID, :inID, :label)";
    @Override
    public RdbmsEdge add(long outVertexID, long inVertexID, final String label) {

        try (Connection con = sql2o.open()) {
            Long genID = con.createQuery(ADD_QUERY, "add edge", true)
                               .addParameter("outID", outVertexID)
                               .addParameter("inID", inVertexID)
                               .addParameter("label", label)
                               .executeUpdate()
                               .getKey(Long.class);
            log.info("generated edge id returned {}", genID);
            return new RdbmsEdge(genID, outVertexID, inVertexID, label, graph);
        }
    }
    // =================================
    private static final String GET_QUERY =
            "select id, out_vertex_id, in_vertex_id, label " +
                    "from edge where id = :id";
    @Override
    public RdbmsEdge get(long id) {
        try (Connection con = sql2o.open()) {
            RdbmsEdge e = con.createQuery(GET_QUERY, "get edge "+id)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeEdge);
            log.info("returned edge for {}: {}", id, e);
            return e;
        }
    }
    // =================================
    // TODO: make this an inner join?
    // TODO: make this a sole responsibility?
    private static final String DELETE_QUERY = "delete from edge where id = :id";
    private static final String DELETE_PROPERTY_QUERY = "delete from property where element_id = :id and type = :type";
    @Override
    public void remove(long id) {
        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("remove edge w id {}", id);
            con.createQuery(DELETE_QUERY, "remove edge "+id)
                    .addParameter("id", id)
                    .executeUpdate();
            con.createQuery(DELETE_PROPERTY_QUERY, "remove properties for "+id)
                    .addParameter("id", id)
                    .addParameter("type", RdbmsElement.PropertyType.EDGE)
                    .executeUpdate();
        }
    }
    // =================================
    private static final String LIST_QUERY =
            "select id, out_vertex_id, in_vertex_id, label from edge";
    @Override
    public Iterable<RdbmsEdge> list() {
        try (org.sql2o.Connection con = sql2o.open()) {
            log.info("request all edges");
            return con.createQuery(LIST_QUERY, "get all edges")
                      .executeAndFetch(makeEdge);
        }
    }
    // =================================
    private static final String FILTER_QUERY =
            "select e.id, e.out_vertex_id, e.in_vertex_id, e.label " +
                    "from edge e, property p " +
                    "where e.id = p.element_id " +
                    "and p.key = :key " +
                    "and p.value = :value " +
                    "and p.type = :type";
    @Override
    public Iterable<RdbmsEdge> list(String key, Object value) {
        String serializedValue = serializer.serialize(value);
        try (Connection con = sql2o.open()) {
            return con.createQuery(FILTER_QUERY, "filtered edges")
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
                    .addColumnMapping("element_id", "id")
                    .addParameter("type", RdbmsElement.PropertyType.EDGE)
                    .executeAndFetch(makeEdge);
        }
    }
    // =================================
    private final static String LIST_VERTEX_QUERY =
            "select id, out_vertex_id, in_vertex_id, label " +
                    "from edge where out_vertex_id = :id or in_vertex_id = :id";
    @Override
    public Iterable<RdbmsEdge> list(Long vertexId) {
        try (Connection con = sql2o.open()) {
            return con.createQuery(LIST_VERTEX_QUERY, "get edges by vertex "+vertexId)
                    .addParameter("id", vertexId)
                    .executeAndFetch(makeEdge);
        }
    }
    // =================================
    private static final String CLEAR_QUERY = "truncate table edge restart identity and commit no check";
    @Override
    public void clear() {
        try (Connection con = sql2o.open()) {
            con.createQuery(CLEAR_QUERY, "clear edges").executeUpdate();
        }
    }
    // =================================
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
    private final Serializer serializer;
}
