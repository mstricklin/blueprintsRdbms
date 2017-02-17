// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;


import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;

public interface DaoFactory {
    interface VertexDao {
        void clear();
        RdbmsVertex add();
        RdbmsVertex get(Object id);
        void remove(Object id);
        Iterable<? extends Vertex> list();
        Iterable<? extends Vertex> list(String key, Object value);
    }
    // =================================
    interface EdgeDao {
        void clear();
        RdbmsEdge add(Vertex outVertex, Vertex inVertex, String label);
        RdbmsEdge get(Object id);
        void remove(Object id);
        Iterable<? extends Edge> list();
        Iterable<? extends Edge> list(String key, Object value);
    }
    // =================================
    interface PropertyDao {
        void clear();
    	void set(Object id, String key, Object value);
    	<T> T get(Object id, String key);
    	void remove(Object id, String key);
    	ImmutableMap<String, Object> properties(Object id);
    }
    // =================================
    interface SerializerDao {
        Map<String, Integer> loadRegistrations();
        void addRegistration(String clazzname, Integer id);
    }
    // =================================

    VertexDao   getVertexDao();
    EdgeDao     getEdgeDao();
    PropertyDao getPropertyDao();
    void close();

}
