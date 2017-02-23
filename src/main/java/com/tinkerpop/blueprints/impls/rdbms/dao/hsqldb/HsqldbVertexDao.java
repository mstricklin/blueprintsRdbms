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

import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.VertexDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbVertexDao implements VertexDao {

    HsqldbVertexDao(DataSource dataSource, RdbmsGraph graph, Serializer serializer_) {
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
        serializer = serializer_;
    }
    // =================================
    private ResultSetHandler<RdbmsVertex> makeVertex = new ResultSetHandler<RdbmsVertex>() {
        @Override
        public RdbmsVertex handle(ResultSet rs) throws SQLException {
            return new RdbmsVertex(rs.getLong(1), graph);
        }
    };
    // =================================
    private static final String ADD_QUERY = "insert into vertex values (null)";
    @Override
    public RdbmsVertex add() {
        try (Connection con = sql2o.open()) {
            Long genID = con.createQuery(ADD_QUERY, "add vertex", true)
                               .executeUpdate()
                               .getKey(Long.class);
            log.info("generated vertex id returned {}", genID);
            return new RdbmsVertex(genID, graph);
        }
    }
    // =================================
    private static final String GET_QUERY = "select id from vertex where id = :id";
    @Override
    public RdbmsVertex get(long id) {
        try (Connection con = sql2o.open()) {
            RdbmsVertex v = con.createQuery(GET_QUERY, "get vertex "+id)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeVertex);
            log.info("returned vertex for {}: {}", id, v);
            return v;
        }
    }
    // =================================
    // TODO: make this an inner join?
    private static final String DELETE_QUERY = "delete from vertex where id = :id";
    private static final String DELETE_PROPERTY_QUERY = "delete from property where element_id = :id and type = :type";
    @Override
    public void remove(long id) {
        try (Connection con = sql2o.open()) {
            log.info("remove vertex w id {}", id);
            con.createQuery(DELETE_QUERY, "remove vertex "+id)
                    .addParameter("id", id)
                    .executeUpdate();
            con.createQuery(DELETE_PROPERTY_QUERY, "remove properties for "+id)
                    .addParameter("id", id)
                    .addParameter("type", RdbmsElement.PropertyType.VERTEX)
                    .executeUpdate();
        }
    }
    // =================================
    private static final String LIST_QUERY = "select id from vertex";
    @Override
    public Iterable<RdbmsVertex> list() {
        try (Connection con = sql2o.open()) {
            return con.createQuery(LIST_QUERY, "get all vertices").executeAndFetch(makeVertex);
        }
    }
    // =================================
    private static final String FILTER_QUERY = "select distinct element_id from property where key = :key and value = :value";
    @Override
    public Iterable<RdbmsVertex> list(String key, Object value) {
        String serializedValue = serializer.serialize(value);
        try (Connection con = sql2o.open()) {
            return con.createQuery(FILTER_QUERY, "filtered vertices")
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
                    .addColumnMapping("element_id", "id")
                    .executeAndFetch(makeVertex);
        }
    }
    // =================================
    private final static String CLEAR_QUERY = "truncate table vertex restart identity and commit no check";
    @Override
    public void clear() {
        try (Connection con = sql2o.open()) {
            con.createQuery(CLEAR_QUERY, "clear vertices").executeUpdate();
        }
    }
    // =================================
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
    private final Serializer serializer;
}
