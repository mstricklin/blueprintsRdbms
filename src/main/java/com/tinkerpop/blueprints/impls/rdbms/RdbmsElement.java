// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Maps.newHashMap;

import java.util.*;

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
        Map<String,Object> p = graph.getProperties(id);
        return (T) p.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
        Map<String,Object> p = graph.getProperties(id);
        return Collections.unmodifiableSet(p.keySet());
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);

        Map<String,Object> p = graph.getProperties(id);

        log.info("set prop for id {}: {}=>{}", id, key, value);
    	Object v = p.get(key);
    	if (null != v)
    	    log.info("prop class {}", p.get(key).getClass().getName());
    	if (Objects.equals(v, value)) {
    		log.info("property already exists, returning {}=>{}", key, value);
    		return;
    	}
        dao.set(id, key, value);
        p.put(key, value);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(String key) {
        Map<String,Object> p = graph.getProperties(id);
    	if (p.containsKey(key)) {
    	    dao.remove(id, key);
            return (T) p.remove(key);
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
    protected final RdbmsGraph graph;
    protected final PropertyDao dao;
    protected final long id;
    // =================================
    @RequiredArgsConstructor(staticName = "of")
    @Data
    public static final class PropertyDTO {
        public final String key;
        public final Object value;
        public static Map<String, Object> toMap(Collection<PropertyDTO> c) {
            Map<String, Object> m = newHashMap();
            for (PropertyDTO p: c)
                m.put(p.key, p.value);
            return m;
        }
    }

}
