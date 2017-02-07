// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;

import static com.google.common.collect.Maps.newConcurrentMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.BaseEncoding;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.SerializerDao;

public class KryoSerializer implements Serializer {

    public KryoSerializer(SerializerDao sd_) {
        // TODO: should be thread-local. How handle registrations...?
        kryo = new Kryo();
        sd = sd_;
        sd.loadRegistrations(); // return map to add to kryo
    }
    // =================================
    @Override
    public <T> String serialize(T o) {
        sd.addRegistration(o);

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
    private final SerializerDao sd;
    private final Map<String, Integer> classMap = newConcurrentMap();

}
