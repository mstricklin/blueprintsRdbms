package com.tinkerpop.blueprints.impls.rdbms;

import com.google.common.cache.*;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.ImmutableSet;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Maps.newHashMap;

@Slf4j
@RequiredArgsConstructor(staticName = "of")
public class PropertyStore {

    // =================================
    static PropertyStore vertexPropertyStore(DaoFactory.PropertyDao dao_) {
        return new PropertyStore(RdbmsElement.PropertyType.VERTEX, dao_);
    }
    static PropertyStore edgePropertyStore(DaoFactory.PropertyDao dao_) {
        return new PropertyStore(RdbmsElement.PropertyType.EDGE, dao_);
    }
    // =================================
    private PropertyStore(RdbmsElement.PropertyType type_, DaoFactory.PropertyDao dao_) {
        type = type_;
        dao = dao_;

        propertyCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .maximumSize(20000)  //TODO: parameterize
                .removalListener(removalListener)
                .build(propertyLoader);
    }
    // =================================
    @SuppressWarnings("unchecked")
    <T> T getProperty(Long id, String key) {
        try {
            return (T) propertyCache.get(id).get(key);
        } catch (ExecutionException | InvalidCacheLoadException e) {
            log.error("could not find properties w id {}", id);
        }
        return null;
    }
    // =================================
    Set<String> getPropertyKeys(Long id) {
//        return Collections.unmodifiableSet( getProperties(id).keySet() );
        return ImmutableSet.copyOf( getProperties(id).keySet() );
    }
    // =================================
    // gonna be a lot of auto-boxing through this call...
    void setProperty(Long id, String key, Object value) {
        // TODO: can key be null?
        // TODO: can value be null?
        validateProperty(key, value);

        Map<String, Object> p = getProperties(id);

        log.info("set prop for id {}: {}=>{}", id, key, value);
        Object v = p.get(key);
        if (null != v)
            log.info("prop class {}", p.get(key).getClass().getName());
        if (Objects.equals(v, value)) {
            log.info("property already exists, returning {}=>{}", key, value);
            return;
        }
        dao.setProperty(id, key, value);
        p.put(key, value);
    }
    // =================================
    Map<String, Object> getProperties(Long id) {
        try {
            return propertyCache.get(id);
        } catch (ExecutionException | InvalidCacheLoadException e) {
            log.error("could not find properties w id {}", id);
        }
        return null;
    }
    // =================================
    // adapted from ElementHelper...
    private final void validateProperty(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw ExceptionFactory.propertyValueCanNotBeNull();
        if (null == key)
            throw ExceptionFactory.propertyKeyCanNotBeNull();
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
        if (type == RdbmsElement.PropertyType.EDGE && key.equals(StringFactory.LABEL))
            throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
        if (key.isEmpty())
            throw ExceptionFactory.propertyKeyCanNotBeEmpty();
    }
    // =================================
    @SuppressWarnings("unchecked")
    <T> T removeProperty(Long id, String key) {
        Map<String, Object> p = getProperties(id);
        if (p.containsKey(key)) {
            dao.remove(id, key);
            log.info("removing {}|{} from cache", id, key);
            return (T) p.remove(key);
        }
        return null;
    }
    // =================================
    void remove(Long id) {
        propertyCache.invalidate(id);
    }
    // =================================
    void clear() {
        propertyCache.invalidateAll();
    }
    // =================================
    private CacheLoader<Long, Map<String, Object>> propertyLoader = new CacheLoader<Long, Map<String, Object>>() {
        @Override
        public Map<String, Object> load(Long id) throws Exception {
            log.info("using cacheLoader for vertex {}", id);
            return PropertyDTO.toMap(dao.properties(id));
        }
    };
    RemovalListener<Long, Map<String, Object>> removalListener = new RemovalListener<Long, Map<String, Object>>() {
        public void onRemoval(RemovalNotification<Long, Map<String, Object>> notification) {
            if (notification.getCause() == RemovalCause.EXPLICIT) {
                dao.remove(notification.getKey());
            }
        }
    };
    // =================================
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
    // =================================

    private final LoadingCache<Long, Map<String, Object>> propertyCache;

    final RdbmsElement.PropertyType type;
    protected final DaoFactory.PropertyDao dao;
}
