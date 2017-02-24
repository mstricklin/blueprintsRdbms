// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;

import static com.google.common.collect.Maps.newConcurrentMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.BaseEncoding;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.SerializerDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KryoSerializer implements Serializer {

    public KryoSerializer(SerializerDao sd_) {
        // TODO: should be thread-local(?) How handle registrations on a per-thread basis?
        kryo = new Kryo();
        serializerDao = sd_;

        // load kryo built-in registrations into our cache
        for (int i = 0; i < kryo.getNextRegistrationId(); ++i) {
            Registration r = kryo.getRegistration(i);
            log.info("KryoSerializer default registration {}", r);
            if (null != r) {
                classRegistrations.put(r, r.getId());
            }
        }

        // load our saved registrations into our cache
        // TODO: what if kryo doesn't agree with our requested registration id?
        for (Map.Entry<String, Integer> e: serializerDao.loadRegistrations().entrySet()) {
            try {
                Class<?> clazz = this.getClass().getClassLoader().loadClass(e.getKey());
                Registration r = kryo.register(clazz, e.getValue());
                log.info("KryoSerializer loaded registration {} => {}", e.getKey(), e.getValue());
                log.info("KryoSerializer loaded registration {}", r);
                classRegistrations.put(r, r.getId());

            } catch (ClassNotFoundException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }
    // =================================
    CacheLoader<Registration, Integer> cl = new CacheLoader<Registration, Integer>() {
        @Override
        public Integer load(Registration r) throws Exception {
            log.info("first time seen serialization for {}", r.getType().getName());
            serializerDao.addRegistration(r.getType().getName(), r.getId());
            return r.getId();
        }
    };
    // =================================
    @Override
    public <T> String serialize(T o) {
        // getRegistration registers if not already
        Registration r = kryo.register(o.getClass());
        log.info("pulled from kryo {} {}", r, r.getId());
        //c.get(r);
        if ( ! classRegistrations.containsKey(r)) {
            log.info("first time seen serialization for {}", r.getType().getName());
            classRegistrations.put(r, r.getId());
            serializerDao.addRegistration(r.getType().getName(), r.getId());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, o);
        }
        return BaseEncoding.base64().encode(baos.toByteArray());
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(String repr) {
        byte[] bytes = BaseEncoding.base64().decode(repr);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (Input input = new Input(bais)) {
            return (T) kryo.readClassAndObject(input);
        }
    }
    // =================================
    private final Kryo kryo;
    private final SerializerDao serializerDao;
    private final Map<Registration, Integer> classRegistrations = newConcurrentMap();
    private final LoadingCache<Registration, Integer> c = CacheBuilder.newBuilder().build(cl);

}
