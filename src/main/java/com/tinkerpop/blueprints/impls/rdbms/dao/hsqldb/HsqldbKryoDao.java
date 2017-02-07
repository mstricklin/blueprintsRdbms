// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;

import javax.sql.DataSource;

import org.codehaus.groovy.runtime.metaclass.NewInstanceMetaMethod;
import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.SerializerDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbKryoDao implements SerializerDao {

    HsqldbKryoDao(DataSource dataSource) {
        ds = dataSource;
        sql2o = new Sql2o(dataSource);
        // TODO: should be thread-local. How handle registrations...?
        kryo = new Kryo();
        loadRegistrations();
    }
    // =================================
    // Oh, we're so overdue for lambdas...
    private final ResultSetHandler<Map.Entry<String, Integer>> makeMakeEntry
            = new ResultSetHandler<Map.Entry<String, Integer>>() {
        @Override
        public Map.Entry<String, Integer> handle(ResultSet rs) throws SQLException {
            return new SimpleImmutableEntry<String, Integer>(rs.getString(1), rs.getInt(2));
        }
    };
    // =================================
    @Override
    public Map<String, Integer> loadRegistrations() {
        String sql = "select classname, id from serial_mapping";
        try (Connection con = sql2o.open()) {
            List<Map.Entry<String, Integer>> entries = con.createQuery(sql, "all serialization mappings")
                                                          .executeAndFetch(makeMakeEntry);
            return ImmutableMap.copyOf( entries );
        }
    }
    // =================================
    // needs to be an upsert...
    @Override
    public void addRegistration(String clazzname, Integer id) {
        log.info("registration not found for class {}", clazzname);
        //Integer id = Integer.valueOf(kryo.register(o.getClass()).getId());
        log.info("mapping to id {}", id);
        classMap.put(clazzname, id);
        String sql = "insert into serial_mapping values (:classname, :id)";
        try (Connection con = sql2o.open()) {
            con.createQuery(sql, "add serialization mapping "+clazzname)
                                .addParameter("classname", clazzname)
                                .addParameter("id", id)
                                .executeUpdate();
        }

    }
    // =================================
    private final DataSource ds;
    private final Sql2o sql2o;
    private final Kryo kryo;
    Map<String, Integer> classMap = newConcurrentMap();
}
