// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.VertexDao;

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
            return new RdbmsVertex(rs.getLong(1), graph);
        }
    };
    // =================================
    @Override
    public void clear() {
        String sql = "truncate table vertex restart identity and commit no check";
        try (Connection con = sql2o.open()) {
            con.createQuery(sql, "clear vertices").executeUpdate();
        }
    }
    // =================================
    @Override
    public RdbmsVertex add() {
        String sql = "insert into vertex values (null)";

        try (Connection con = sql2o.open()) {
            Long genID = con.createQuery(sql, "add vertex", true)
                               .executeUpdate()
                               .getKey(Long.class);
            log.info("generated vertex id returned {}", genID);
            return new RdbmsVertex(genID.longValue(), graph);
        }
    }
    // =================================
    @Override
    public RdbmsVertex get(long id) {
        // TODO: this needs to be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select * from vertex where id = :id";

        try (Connection con = sql2o.open()) {
            RdbmsVertex v = con.createQuery(sql, "get vertex "+id)
                               .addParameter("id", id)
                               .executeAndFetchFirst(makeVertex);
            log.info("returned vertex for {}: {}", id, v);
            return v;
        }
    }
    // =================================
    @Override
    public void remove(long id) {

        String sql0 = "delete from vertex where id = :id";
        String sql1 = "delete from property where element_id = :id";

        try (Connection con = sql2o.open()) {
            log.info("remove vertex w id {}", id);
            con.createQuery(sql0, "remove vertex "+id)
                          .addParameter("id", id).executeUpdate();
            con.createQuery(sql1, "remove properties for "+id)
                          .addParameter("id", id).executeUpdate();
        }
    }
    // =================================
    @Override
    public Iterable<RdbmsVertex> list() {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        String sql = "select id from vertex";

        try (Connection con = sql2o.open()) {
            return con.createQuery(sql, "get all vertices").executeAndFetch(makeVertex);
        }
    }
    // =================================
    @Override
    public Iterable<RdbmsVertex> list(String key, Object value) {
        // TODO: this can be heavily optimized...any time one does a 'getProperty' on each
        // vertex in turn, it adds another round-trip to the DB
        // TODO: how to we serialize properties?
        String sql = "select distinct element_id from property where key = :key and value = :value";

        try (Connection con = sql2o.open()) {
            return con.createQuery(sql, "filtered vertices")
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
