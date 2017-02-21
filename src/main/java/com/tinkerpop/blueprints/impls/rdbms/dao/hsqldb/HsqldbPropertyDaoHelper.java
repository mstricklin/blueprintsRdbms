// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import javax.sql.DataSource;
import java.util.List;


// There are two tables with exactly the same structure: vertex_property and edge_property
@Slf4j
public class HsqldbPropertyDaoHelper implements PropertyDao {

    // =================================
    HsqldbPropertyDaoHelper(RdbmsElement.PropertyType type_, DataSource dataSource_, Serializer serializer_) {
        type = type_;
        sql2o = new Sql2o(dataSource_);
        serializer = serializer_;
    }
    // =================================
    @Override
    public void setProperty(long id, String key, Object value) {
        // TODO: clean up serialization
        String serializedValue = serializer.serialize(value);
        log.info("serialized version of {} is '{}'", value, serializedValue);

        try (Connection con = sql2o.open()) {
            con.createQuery(SET_QUERY, "update property")
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .addParameter("key", key)
                    .addParameter("value", serializedValue)
                    .executeUpdate();
            log.info("set property {}={} for element id {}", key, value, id);
        }
    }
    // =================================
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
    public List<RdbmsElement.PropertyDTO> properties(long id) {
        try (Connection con = sql2o.open()) {
            log.info("get all properties for {}", id);
            return con.createQuery(GET_ALL_QUERY, "get all properties "+id)
                    .addParameter("id", id)
                    .addParameter("type", type)
                    .executeAndFetch(RdbmsElement.PropertyDTO.class);
        }
    }
    // =================================
    private final Sql2o sql2o;
    private final Serializer serializer;
    private final RdbmsElement.PropertyType type; // the type of element targeted: 'V' or 'E'
    // =================================
    private final static String SET_QUERY = "MERGE INTO property AS p " +
            "USING (VALUES(:id, :type, :key, :value)) AS r(id,type,key,value) " +
            "ON p.element_id = r.id AND p.type = t.type AND p.key = r.key " +
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
