// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Maps.newHashMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.util.ElementHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RdbmsElement implements Element {

    RdbmsElement(final int id_, final RdbmsGraph graph_) {
        id = id_;
        graph = graph_;
        dao = graph.getDaoFactory().getPropertyDao();
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
    	log.info("get property, dao {}", dao);
    	populate();
        return (T) properties_.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
    	populate();
        return new HashSet<String>(properties_.keySet());
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);

        log.info("set property, dao {}", dao);
        log.info("set prop for id {}: {}=>{}", id, key, value);
    	populate();
    	Object v = properties_.get(key);
    	if (null != v)
    	    log.info("prop class {}", properties_.get(key).getClass().getName());
    	if (Objects.equals(properties_.get(key), value)) {
    		log.info("property already set, returning {}=>{}", key, value);
    		return;
    	}
        dao.set(Integer.valueOf(id), key, value);
        properties_.put(key, value);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
    	populate();
    	if (properties_.containsKey(key)) {
    	    dao.remove(Integer.valueOf(id), key);
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
        return Integer.valueOf(this.id);
    }
    protected int rawId() {
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
        return this.id;
    }
    // =================================
    private void populate() {
        // TODO: ain't thread-safe
    	log.info("Element populate {}", id);
    	if (populated_)
    		return;
    	properties_ = dao.values(Integer.valueOf(id));
    	populated_ = true;
    }
    // =================================
    protected final RdbmsGraph graph;
    protected final PropertyDao dao;
    protected final int id;
    protected boolean populated_ = false;
    Map<String, Object> properties_ = newHashMap();

}
