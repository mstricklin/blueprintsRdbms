// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.util.ElementHelper;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RdbmsElement implements Element {

//    static public ResultSetHandler<Long> getID() {
//        return new ResultSetHandler<Long>() {
//            @Override
//            public RdbmsElement handle(ResultSet rs) throws SQLException {
//                return rs.next() ? new RdbmsElement(rs.getLong(1), g)
//                                 : null;
//            }
//        };
//    }



    RdbmsElement(final int id_, final RdbmsGraph graph_) {
        id = id_;
        graph = graph_;
        dao = graph.getDaoFactory().getPropertyDao();
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
    	log.info("get property, dao {}", dao);
    	T value = dao.get(Integer.valueOf(id), key);
    	log.info("got prop for id {}: {}={}", id, key, value);
//      if not populated
//          populate();
        return (T) properties_.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
//        if not populated:
//            populate();
//        return properties.keys
        return new HashSet<String>(properties_.keySet());
    }
    // =================================
    @Override
    public void setProperty(String key, Object value) {
//        if not populated:
//            push value
//            populate
//        else:
//            check if value is the same as cached
//            if no:
//                push value
//                set into map

        ElementHelper.validateProperty(this, key, value);
        
        log.info("set property, dao {}", dao);
        log.info("set prop for id {}: {}={}", id, key, value);
    	dao.set(Integer.valueOf(id), key, value);
        
//        properties_.put(key, value);
//        populate();
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
//        if populated
//            remove for map
//            remove from db
//        else
//            remove from db
//            populate
        return (T) properties_.remove(key);
    }
    // =================================
    @Override
    public abstract void remove();
    // =================================
    @Override
    public Object getId() {
        return Integer.valueOf(this.id);
    }
    protected int rawId() {
        return this.id;
    }
    // =================================
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    // =================================
    protected Graph getGraph() {
        return graph;
    }
    // =================================
    @Override
    public int hashCode() {
        return this.id;
    }
    // =================================
    @Data
    public static class Property {
        String key;
        String value;
        static ResultSetHandler<Property> handler = new BeanHandler<Property>(Property.class);
        static ResultSetHandler<List<Property>> listHandler = new BeanListHandler<Property>(Property.class);
    }
    // =================================
    private void populate() {
        // TODO: ain't thread-safe
        QueryRunner qr = new QueryRunner();
        try (Connection conn = graph.dbConn()) {
            log.info("request properties for id {}", Long.valueOf(this.id));
            List<Property> l = qr.query(conn, "select key, value from property where element_id = ?;",
                    Property.listHandler, Integer.valueOf(this.id));
            for (Property p: l) {
                log.info("Property {}", p);
            }

        } catch (SQLException e) {
            log.error("SQL error", e);
        }
    }
    // =================================
    protected final RdbmsGraph graph;
    protected final PropertyDao dao;
    protected final int id;
    protected boolean populated = false;
    final Map<String, Object> properties_ = newHashMap();

}
