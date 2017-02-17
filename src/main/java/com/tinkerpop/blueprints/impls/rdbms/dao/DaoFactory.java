// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;


import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;

public interface DaoFactory {
    interface VertexDao {
        void clear();
        RdbmsVertex add();
        RdbmsVertex get(long id);
        void remove(long id);
        Iterable<RdbmsVertex> list();
        Iterable<RdbmsVertex> list(String key, Object value);
    }
    // =================================
    interface EdgeDao {
        void clear();
        RdbmsEdge add(long outVertexID, long inVertexID, final String label);
        RdbmsEdge get(long id);
        void remove(long id);
        Iterable<RdbmsEdge> list();
        Iterable<RdbmsEdge> list(String key, Object value);
        Iterable<RdbmsEdge> list(Long vertexId);
    }
    // =================================
    interface PropertyDao {
        void clear();
    	void set(long id, String key, Object value);
    	<T> T get(long id, String key);
    	void remove(long id, String key);
    	ImmutableMap<String, Object> properties(long id);
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
