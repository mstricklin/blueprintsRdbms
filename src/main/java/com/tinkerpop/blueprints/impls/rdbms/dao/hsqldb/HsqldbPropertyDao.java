package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;


import java.util.List;

import javax.sql.DataSource;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.impls.rdbms.PropertyStore;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import lombok.Data;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.collect.Lists.transform;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(RdbmsElement.PropertyType type_, DataSource dataSource_, Serializer serializer_) {
	    type = type_;
        sql2o = new Sql2o(dataSource_);
        serializer = serializer_;
    }
	// =================================
    private static final String UPSERT_QUERY = "MERGE INTO property AS p " +
            "USING (VALUES(:id, :type, :key, :value)) AS r(id,type,key,value) " +
            "ON p.element_id = r.id AND p.type = r.type AND p.key = r.key " +
            "WHEN MATCHED THEN " +
            "      UPDATE SET p.value = r.value " +
            "WHEN NOT MATCHED THEN " +
            "    INSERT VALUES r.id, r.type, r.key, r.value";
    @Override
	public void setProperty(long id, String key, Object value) {
        String serializedValue = serializer.serialize(value);
        try (Connection con = sql2o.open()) {
            con.createQuery(UPSERT_QUERY, "upsert property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
                    .executeUpdate();
            log.debug("set property {}={} for element id {}", key, value, id);
        }
	}
	// =================================
    // This is probably not needed, since it's very close to as cheap to get
    // all of the properties for an element as it is to get one.
    private static final String GET_QUERY = "select value from property where element_id = :id and type = :type and key = :key";
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
            Object o = serializer.deserialize(value);
            log.debug("returned prop for {} {}: {} ({})", id, key, value, o);
            return (T)o;
        }
	}
    // =================================
    private static final String CLEAR_QUERY = "delete from property " +
            "where element_id = :id and type = :type";
    @Override
    public void remove(long id) {
        log.debug("clear property {}", id);
        try (Connection con = sql2o.open()) {
            con.createQuery(CLEAR_QUERY, "clear property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .executeUpdate();
        }
    }
	// =================================
    private static final String RM_QUERY = "delete from property " +
            "where element_id = :id and type = :type and key = :key";
    @Override
	public void remove(long id, String key) {
	    log.debug("remove property {} => {}", id, key);
        try (Connection con = sql2o.open()) {
            con.createQuery(RM_QUERY, "remove property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .addParameter("key", key)
                    .executeUpdate();
        }
	}
	// =================================
    private static final String GET_ALL_QUERY = "select key, value from property " +
            "where element_id = :id and type = :type";
    @Override
	public List<PropertyStore.PropertyDTO> properties(long id) {
        @Data
        final class Prop {
            public final String key;
            public final String value;
        }
        try (Connection con = sql2o.open()) {
            log.debug("get all properties for {}", id);
            List<Prop> l = con.createQuery(GET_ALL_QUERY, "get all properties "+id)
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .executeAndFetch(Prop.class);

            return transform(l, new Function<Prop, PropertyStore.PropertyDTO>() {

                @Override
                public PropertyStore.PropertyDTO apply(Prop p) {
                    return PropertyStore.PropertyDTO.of(p.key, serializer.deserialize(p.value));
                }
            });
        }
	}
    // =================================
    private static final String sqlClear = "truncate table property restart identity and commit no check";
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
    private final RdbmsElement.PropertyType type;
    private final Sql2o sql2o;
    private final Serializer serializer;
    // =================================
    }
