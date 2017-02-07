package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(DataSource dataSource_, RdbmsGraph graph_, Serializer serializer_) {
        sql2o = new Sql2o(dataSource_);
        serializer = serializer_;
    }
    // =================================
	@Override
	public void set(Object id, String key, Object value) {
	    // TODO: clean up serialization
	    String serializedValue = serializer.serialize(value);
	    log.info("serialized verion of {} is '{}'", value, serializedValue);

		String sql = "MERGE INTO property AS p " +
		             "USING (VALUES(:id, :key, :value)) AS r(id,key,value) " +
		             "ON p.element_id = r.id AND p.key = r.key " +
		             "WHEN MATCHED THEN " +
		             "      UPDATE SET p.value = r.value " +
		             "WHEN NOT MATCHED THEN " +
		             "    INSERT VALUES r.id, r.key, r.value";
		try (Connection con = sql2o.open()) {
			con.createQuery(sql, "update property")
                    .addParameter("id", id)
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
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
            String value = con.createQuery(sql, "get property")
                               .addParameter("id", id)
                               .addParameter("key", key)
                               .executeScalar(String.class);
            log.info("returned prop for {} {}: {}", id, key, value);
            Object o = serializer.deserialize(value);
            log.info("deserialized: {}", o);
            return (T)o;
        }
	}
	// =================================
	@Override
	public void remove(Object id, String key) {
		String sql = "delete from property " +
		             "where element_id = :id and key = :key";
		try (Connection con = sql2o.open()) {
            con.createQuery(sql, "remove property")
                                .addParameter("id", id)
                                .addParameter("key", key)
                                .executeUpdate();
            log.info("deleted prop for {} {}", id, key);
        }
	}
	// =================================
    private final ResultSetHandler<Map.Entry<String, Object>> makeProperty
            = new ResultSetHandler<Map.Entry<String, Object>>() {
        @Override
        public Map.Entry<String, Object> handle(ResultSet rs) throws SQLException {
            Object o = serializer.deserialize(rs.getString(2));
            return new SimpleImmutableEntry<String, Object>(rs.getString(1), o);
        }
    };
	// =================================
	@Override
	public ImmutableMap<String, Object> properties(Object id) {
        String sql = "select key, value from property " +
	                 "where element_id = :id";

        try (Connection con = sql2o.open()) {
        	log.info("get all properties for {}", id);
            List<Map.Entry<String, Object>> entryList = con.createQuery(sql, "get all properties "+id)
            		              .addParameter("id", id)
                                  .executeAndFetch(makeProperty);
            // TODO: Need to deserialize...
            ImmutableMap<String, Object> entryMap = ImmutableMap.copyOf( entryList );
            log.info("props: {}", entryMap);
//            return m;
            return entryMap;
        }
	}
	// =================================
    private final Sql2o sql2o;
    private final Serializer serializer;
}
