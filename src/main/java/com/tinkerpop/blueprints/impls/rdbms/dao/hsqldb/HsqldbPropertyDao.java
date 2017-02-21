package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.util.List;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.impls.rdbms.PropertyStore;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(RdbmsElement.PropertyType type_, DataSource dataSource_, Serializer serializer_) {
	    type = type_;
        sql2o = new Sql2o(dataSource_);
        serializer = serializer_;
    }
	// =================================
	@Override
	public void setProperty(long id, String key, Object value) {
        String serializedValue = serializer.serialize(value);
        try (Connection con = sql2o.open()) {
            con.createQuery(SET_QUERY, "upsert property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
                    .executeUpdate();
            log.info("set property {}={} for element id {}", key, value, id);
        }
	}
	// =================================
    // This is probably not needed, since it's very close to as cheap to get
    // all of the properties for an element as it is to get one.
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getProperty(long id, String key) {
        log.info("property get {} {}", id, key);
        try (Connection con = sql2o.open()) {
            String value = con.createQuery(GET_QUERY, "get property")
                    .addParameter("id", id)
                    .addParameter("type", type)
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
	public void removeProperty(long id, String key) {
	    log.info("remove property {} => {}", id, key);
        try (Connection con = sql2o.open()) {
            con.createQuery(RM_QUERY, "remove property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .addParameter("key", key)
                    .executeUpdate();
            log.info("deleted prop for {} {}", id, key);
        }
	}
	// =================================
	@Override
	public List<PropertyStore.PropertyDTO> properties(long id) {
        try (Connection con = sql2o.open()) {
            log.info("get all properties for {}", id);
            return con.createQuery(GET_ALL_QUERY, "get all properties "+id)
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .executeAndFetch(PropertyStore.PropertyDTO.class);
        }
	}
    // =================================
    private final static String sqlClear = "truncate table property restart identity and commit no check";
    @Override
    public void clear() {
        try (Connection con = sql2o.open()) {
            con.createQuery(sqlClear, "clear properties").executeUpdate();
        }
    }
    static void clear(DataSource dataSource_) {
        try (Connection con = new Sql2o(dataSource_).open()) {
            con.createQuery(sqlClear, "clear properties").executeUpdate();
        }
    }
	// =================================
    RdbmsElement.PropertyType type;
    private final Sql2o sql2o;
    private final Serializer serializer;
    // =================================
    private final static String SET_QUERY = "MERGE INTO property AS p " +
            "USING (VALUES(:id, :type, :key, :value)) AS r(id,type,key,value) " +
            "ON p.element_id = r.id AND p.type = r.type AND p.key = r.key " +
            "WHEN MATCHED THEN " +
            "      UPDATE SET p.value = r.value " +
            "WHEN NOT MATCHED THEN " +
            "    INSERT VALUES r.id, r.type, r.key, r.value";
    private final static String GET_QUERY = "select value from property where element_id = :id and type = :type and key = :key";
    private final static String RM_QUERY = "delete from property " +
            "where element_id = :id and type = :type and key = :key";
    private final static String GET_ALL_QUERY = "select key, value from property " +
            "where element_id = :id and type = :type";
}
