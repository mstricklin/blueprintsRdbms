package com.tinkerpop.blueprints.impls.rdbms;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tinkerpop.blueprints.util.ElementHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

@Slf4j
public class PropertyStore {
    public static enum ElementType {
        VERTEX("V"),
        EDGE("E");

        ElementType(final String s_) { s=s_; }
        @Override
        public String toString() { return s; }
        public final String s;
    }

    PropertyStore(ElementType type_) {
        type = type_;

        propertyCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .maximumSize(20000)  //TODO: parameterize
                .build(propertyLoader);
        // get DAO based on type
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(String key) {
        Map<String,Object> p = getGraph().getProperties(id);
        return (T) p.get(key);
    }
    // =================================
    @Override
    public Set<String> getPropertyKeys() {
        Map<String,Object> p = getGraph().getProperties(id);
        return Collections.unmodifiableSet(p.keySet());
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);

        Map<String,Object> p = getGraph().getProperties(id);

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
    }

    private CacheLoader<Long, Map<String, Object>> propertyLoader = new CacheLoader<Long, Map<String, Object>>() {
        @Override
        public Map<String, Object> load(Long id) throws Exception {
            log.info("using cacheLoader for vertexproperty {}", id);
            return newHashMap();
        }
    };

    private final LoadingCache<Long, Map<String, Object>> propertyCache;

    final ElementType type;
}
