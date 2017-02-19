package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.util.List;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(DataSource dataSource_, Serializer serializer_) {
        sql2o = new Sql2o(dataSource_);
        serializer = serializer_;
    }
	// =================================
	@Override
	public void clear() {
		String sql = "truncate table property restart identity and commit no check";
		try (Connection con = sql2o.open()) {
			con.createQuery(sql, "clear properties").executeUpdate();
		}
	}
    // =================================
	@Override
	public void set(long id, String key, Object value) {
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
	public <T> T get(long id, String key) {
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
	public void remove(long id, String key) {
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
	@Override
	public List<RdbmsElement.PropertyDTO> properties(long id) {
        String sql = "select key, value from property " +
	                 "where element_id = :id";

        try (Connection con = sql2o.open()) {
        	log.info("get all properties for {}", id);
            return con.createQuery(sql, "get all properties "+id)
            		              .addParameter("id", id)
                                  .executeAndFetch(RdbmsElement.PropertyDTO.class);
        }
	}
	// =================================
    private final Sql2o sql2o;
    private final Serializer serializer;
}
