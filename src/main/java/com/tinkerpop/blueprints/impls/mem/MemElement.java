// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import static com.google.common.collect.Maps.newHashMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.ElementHelper;

public abstract class MemElement implements Element {

    MemElement(final long id, final MemGraph graph) {
        id_ = id;
        graph_ = graph;
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
        return (T) properties_.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
        return new HashSet<String>(properties_.keySet());
    }
    // =================================
    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);
        properties_.put(key, value);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
        return (T) properties_.remove(key);
    }
    // =================================
    @Override
    public abstract void remove();
    // =================================
    @Override
    public Object getId() {
        return Long.valueOf(id_);
    }
    protected long rawId() {
        return id_;
    }
    // =================================
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    // =================================
    protected Graph getGraph() {
        return graph_;
    }
    // =================================
    @Override
    public int hashCode() {
        return this.getId().hashCode();
    }
    // =================================
    protected final MemGraph graph_;
    protected final long id_;
    final Map<String, Object> properties_ = newHashMap();

}
