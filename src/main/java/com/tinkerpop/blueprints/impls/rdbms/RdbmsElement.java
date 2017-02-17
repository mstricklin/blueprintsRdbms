// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Maps.newHashMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.util.ElementHelper;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RdbmsElement implements Element {

    RdbmsElement(final long id_, final RdbmsGraph graph_) {
        id = id_;
        graph = graph_;
        dao = graph.getDaoFactory().getPropertyDao();
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
    	populate();
        return (T) properties_.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
    	populate();
        return Collections.unmodifiableSet(properties_.keySet());
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);

        populate();

        log.info("set prop for id {}: {}=>{}", id, key, value);
    	Object v = properties_.get(key);
    	if (null != v)
    	    log.info("prop class {}", properties_.get(key).getClass().getName());
    	if (Objects.equals(v, value)) {
    		log.info("property already exists, returning {}=>{}", key, value);
    		return;
    	}
        dao.set(Long.valueOf(id), key, value);
        properties_.put(key, value);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
    	populate();
    	if (properties_.containsKey(key)) {
    	    dao.remove(id, key);
            return (T) properties_.remove(key);
    	}
    	return null;
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
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    // =================================
    protected Graph getGraph() {
        return graph;
    }
    // =================================
    @Override
    public int hashCode() {
        return (int)id;
    }
    // =================================
    private void populate() {
        // TODO: ain't thread-safe
    	if (populated_)
    		return;
    	properties_ = newHashMap( dao.properties(Long.valueOf(id)) );
    	populated_ = true;
    }
    // =================================
    protected final RdbmsGraph graph;
    protected final PropertyDao dao;
    protected final long id;
    protected boolean populated_ = false;
    Map<String, Object> properties_ = newHashMap();
    Map<PropKey, Object> qProperties_ = newHashMap();

    @Data
    @RequiredArgsConstructor(staticName = "of")
    protected static final class PropKey {
        final long id;
        final String key;
    }

}
