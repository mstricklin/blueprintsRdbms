// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.BaseEncoding;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.SerializeDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbSerializerDao implements Serializer, SerializeDao {

    HsqldbSerializerDao(DataSource dataSource) {
        ds = dataSource;
        sql2o = new Sql2o(dataSource);
        // TODO: should be thread-local. How handle registrations...?
        kryo = new Kryo();
        loadRegistrations();
    }
    // =================================
    @Override
    public <T> String
    serialize(T o) {
        addRegistration(o);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, o);
        }
        return BaseEncoding.base64().encode(baos.toByteArray());
    }

    // =================================
    @Override
    @SuppressWarnings("unchecked")
    public <T> T
    deserialize(String repr) {
        byte[] bytes = BaseEncoding.base64().decode(repr);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (Input input = new Input(bais)) {
            return (T) kryo.readClassAndObject(input);
        }
    }
    // =================================
    @Override
    public void loadRegistrations() {
        // get pre-known registrations from Kryo...
        for (int i = 0; i < kryo.getNextRegistrationId(); ++i) {
            Registration r = kryo.getRegistration(i);
            if (null != r)
                classMap.put(r.getClass().getCanonicalName(), Integer.valueOf(r.getId()));
        }

        // get list from DB and merge in...
        class ClassRegistration {
            String clazzname;
            Integer id;
        }
        String sql = "select classname, id from serial_mapping";
        try (Connection con = sql2o.open()) {
            List<ClassRegistration> l = con.createQuery(sql)
                    .executeAndFetch(ClassRegistration.class);
            for (ClassRegistration cr: l) {
                log.info("registration {} => {}", cr.id, cr.clazzname);
                try {
                    Class<?> c = Class.forName(cr.clazzname);
                    log.info("Kryo register {} => {}", cr.clazzname, cr.id);
                    kryo.register(c, cr.id.intValue());
                } catch (ClassNotFoundException e) {
                    log.error("Cannot instantiate {} to register", cr.clazzname);
                    log.error("", e);
                }
            }
        }
    }
    // =================================
    @Override
    public <T> void addRegistration(T o) {
        String clazzname = o.getClass().getCanonicalName();
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
