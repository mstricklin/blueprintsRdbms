package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import static com.google.common.collect.Maps.newHashMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(DataSource dataSource, RdbmsGraph graph) {
        ds = dataSource;
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
    }
    // =================================
	@Override
	public void set(Object id, String key, Object value) {
		// TODO: this needs to be an upsert
		String sql = "MERGE INTO property AS p " +
		             "USING (VALUES(:id, :key, :value)) AS r(id,key,value) " +
		             "ON p.element_id = r.id AND p.key = r.key " +
		             "WHEN MATCHED THEN " +
		             "      UPDATE SET p.value = r.value " +
		             "WHEN NOT MATCHED THEN " +
		             "    INSERT VALUES r.id, r.key, r.value";
		try (Connection con = sql2o.open()) {
			con.createQuery(sql)
                    .addParameter("id", id)
                    .addParameter("key", key)
                    .addParameter("value", value)
					.executeUpdate();
			log.info("set property {}={} for element id {}", key, value, id);
		}
	}
	// =================================
	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Object id, String key) {
		log.info("property get {} {}", id, key);
		String sql = "select value from property where element_id = :id and key = :key";
		try (Connection con = sql2o.open()) {
            String val = con.createQuery(sql)
                               .addParameter("id", id)
                               .addParameter("key", key)
                               .executeScalar(String.class);
            log.info("returned prop for {} {}: {}", id, key, val);
            return (T)val;
        }
	}
	// =================================
	@Override
	public void remove(Object id, String key) {
		String sql = "delete from property " +
		             "where element_id = :id and key = :key";
		try (Connection con = sql2o.open()) {
            con.createQuery(sql).addParameter("id", id)
                                .addParameter("key", key)
                                .executeUpdate();
            log.info("deleted prop for {} {}", id, key);
        }
	}
	// =================================
	@Override
	public Set<String> keys(Object id) {
		// TODO Auto-generated method stub
		return null;
	}
	// =================================
    private static class Property {
		String key;
        Object value;
    }
	// =================================
	@Override
	public Map<String, Object> values(Object id) {
        String sql = "select key, value from property " +
	                 "where element_id = :id";
        
        try (Connection con = sql2o.open()) {
        	log.info("get all properties for {}", id);
            List<Property> l = con.createQuery(sql)
            		              .addParameter("id", id)
                                  .executeAndFetch(Property.class);
            Map<String, Object> m = newHashMap();
            for (Property p: l) {
            	log.info("prop {}=>{}", p.key, p.value);
            	m.put(p.key, p.value);
            }
            return m;
        }
        //return Collections.emptyMap();
	}
	// =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
