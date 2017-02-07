// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;


import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;

public interface DaoFactory {
    interface VertexDao {
        RdbmsVertex add();
        RdbmsVertex get(Object id);
        void remove(Object id);
        Iterable<? extends Vertex> list();
        Iterable<? extends Vertex> list(String key, Object value);
    }
    // =================================
    interface EdgeDao {
        RdbmsEdge add(Vertex outVertex, Vertex inVertex, String label);
        RdbmsEdge get(Object id);
        void remove(Object id);
        Iterable<? extends Edge> list();
        Iterable<? extends Edge> list(String key, Object value);
    }
    // =================================
    interface PropertyDao {
    	void set(Object id, String key, Object value);
    	<T> T get(Object id, String key);
    	void remove(Object id, String key);
    	Set<String> keys(Object id);
		Map<String, Object> values(Object id);
    }
    // =================================
    interface SerializerDao {
        Map<String, Integer> loadRegistrations();
        <T> void addRegistration(T o);
    }
    // =================================

    VertexDao   getVertexDao();
    EdgeDao     getEdgeDao();
    PropertyDao getPropertyDao();

}
