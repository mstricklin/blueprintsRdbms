// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms.dao;


import java.util.List;
import java.util.Map;

import com.tinkerpop.blueprints.impls.rdbms.PropertyStore;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsElement;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsVertex;

public interface DaoFactory {
    // =================================
    interface PropertyDao {
        void setProperty(long id, String key, Object value);
        <T> T getProperty(long id, String key);
        void remove(long id);
        void remove(long id, String key);
        List<PropertyStore.PropertyDTO> properties(long id);
        void clear();
    }
    // =================================
    interface VertexDao {
        RdbmsVertex add();
        RdbmsVertex get(long id);
        void remove(long id);
        Iterable<RdbmsVertex> list();
        Iterable<RdbmsVertex> list(String key, Object value);
        void clear();
    }
    // =================================
    interface EdgeDao {
        RdbmsEdge add(long outVertexID, long inVertexID, final String label);
        RdbmsEdge get(long id);
        void remove(long id);
        Iterable<RdbmsEdge> list();
        Iterable<RdbmsEdge> list(String key, Object value);
        Iterable<RdbmsEdge> list(Long vertexId);
        void clear();
    }
    // =================================
    interface SerializerDao {
        Map<String, Integer> loadRegistrations();
        void addRegistration(String clazzname, Integer id);
    }
    // =================================

    VertexDao   getVertexDao();
    EdgeDao     getEdgeDao();
    PropertyDao getPropertyDao(RdbmsElement.PropertyType type);
    void clear();
    void close();

}
