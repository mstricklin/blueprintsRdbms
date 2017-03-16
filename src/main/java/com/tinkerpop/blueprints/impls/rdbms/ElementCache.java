package com.tinkerpop.blueprints.impls.rdbms;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

@Slf4j
public abstract class ElementCache<E extends IdAble> {
    ElementCache() {
        cache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .maximumSize(20000)  //TODO: parameterize
                .build(propertyLoader);
    }
    // =================================
    E add(E e) {
        cache.put(e.rawId(), e);
        return e;
    }
    // =================================
    E get(Long id) {
        try {
            return cache.get(id);
        } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e)  {
            log.error("could not find element w id {}", id);
        }
        return null;
    }
    // =================================
    void update(Long id) {
    }
    // =================================
    void remove(Long id) {
    }
    // =================================
    private final LoadingCache<Long, E> cache;

}
