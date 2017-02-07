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
    static final class ClassRegistration  {
		String clazzname;
        Integer id;
        static Function<ClassRegistration, Map.Entry<String, Integer>> makeEntry
                 = new Function<ClassRegistration, Map.Entry<String, Integer>>() {
			@Override
			public Entry<String, Integer> apply(ClassRegistration cr) {
				return new SimpleImmutableEntry<String, Integer>(cr.clazzname, cr.id);
			}
        };
    }
    @Override
    public Map<String, Integer> loadRegistrations() {
        String sql = "select classname, id from serial_mapping";
        try (Connection con = sql2o.open()) {
            List<ClassRegistration> l = con.createQuery(sql)
                    .executeAndFetch(ClassRegistration.class);
            return ImmutableMap.copyOf( Iterables.transform(l, ClassRegistration.makeEntry) );
        }
    }
    // =================================
    @Override
    public <T> void addRegistration(T o) {
        String clazzname = o.getClass().getName();
        if (classMap.containsKey(clazzname))
            return;

        log.info("registration not found for class {}", clazzname);
        Integer id = Integer.valueOf(kryo.register(o.getClass()).getId());
        log.info("mapping to id {}", id);
        classMap.put(clazzname, id);
        String sql = "insert into serial_mapping values (:clazzname, :id)";
        try (Connection con = sql2o.open()) {
            con.createQuery(sql).addParameter("clazzname", clazzname).addParameter("id", id).executeUpdate();
        }

    }
    // =================================
    private final DataSource ds;
    private final Sql2o sql2o;
    private final Kryo kryo;
    Map<String, Integer> classMap = newConcurrentMap();
}
