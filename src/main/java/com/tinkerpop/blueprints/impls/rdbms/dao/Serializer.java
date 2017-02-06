// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;

public interface Serializer {

    <T> String serialize(T o);
    <T> T deserialize(String repr);
}