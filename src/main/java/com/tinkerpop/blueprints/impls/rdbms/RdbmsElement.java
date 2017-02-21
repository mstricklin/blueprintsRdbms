// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Maps.newHashMap;

import java.util.*;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.util.ElementHelper;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RdbmsElement implements Element {
    public enum PropertyType {
        VERTEX("V"),
        EDGE("E");

        PropertyType(final String s_) {
            s = s_;
        }
        @Override
        public String toString() {
            return s;
        }
        public final String s;
    }
    // =================================
    RdbmsElement(final PropertyStore cache_, final long id_, final RdbmsGraph graph_) {
        id = id_;
        graph = graph_;
//        dao = graph.getDaoFactory().getPropertyDao(type);
        cache = cache_;
        //graph.vertexPropertyCache();

    }
    // =================================
    @Override
    public abstract void remove();
    // =================================
    @Override
    public Object getId() {
        return this.id;
    }
    protected long rawId() {
        return this.id;
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
        return cache.getProperty(id, key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
        return cache.getPropertyKeys(id);
    }
    // =================================
    @Override
    public void setProperty(String key, Object value) {
        cache.setProperty(id, key, value);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
        return cache.removeProperty(id, key);
    }
    // =================================
    protected RdbmsGraph getGraph() {
        return graph;
    }
    // =================================
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    @Override
    public int hashCode() {
        return (int)id;
    }
    // =================================
    protected final RdbmsGraph graph;
    protected final PropertyStore cache;
    protected final long id;


}
