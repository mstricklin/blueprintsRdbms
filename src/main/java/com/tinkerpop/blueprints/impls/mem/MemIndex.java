// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import static com.google.common.collect.Sets.newHashSet;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.WrappingCloseableIterable;

public class MemIndex <T extends Element> implements Index<T> {

    MemIndex(final String indexName, final Class<T> indexClass) {
        name_ = indexName;
        clazz_ = indexClass;
    }
    // =================================
    @Override
    public String getIndexName() {
        return name_;
    }
    // =================================
    @Override
    public Class<T> getIndexClass() {
        return clazz_;
    }
    // =================================
    @Override
    public void put(String key, Object value, T element) {
        Set<T> s = index_.get(key, value);
        if (null == s) {
            s = newHashSet();
            index_.put(key, value, s);
        }
        s.add(element);
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public CloseableIterable<T> get(String key, Object value) {
        return (index_.contains(key, value))
                           ? new WrappingCloseableIterable<T>(Collections.EMPTY_SET)
                           : new WrappingCloseableIterable<T>(index_.get(key, value));
    }
    // =================================
    @Override
    public CloseableIterable<T> query(String key, Object query) {
        throw new UnsupportedOperationException();
    }
    // =================================
    @Override
    public long count(String key, Object value) {
        Set<T> s = index_.get(key, value);
        return (null == s) ? 0 : s.size();
    }
    // =================================
    @Override
    public void remove(String key, Object value, T element) {
        Set<T> s = index_.get(key, value);
        if (null != s)
            s.remove(element);
    }
    // =================================
    public void remove(String key, Object value) {
        index_.remove(key, value);
    }
    // =================================
    public void remove(String key) {
        index_.row(key).clear();
    }
    // =================================
    private final String name_;
    private final Class<T> clazz_;
    Table<String, Object, Set<T>> index_ = HashBasedTable.create();
}
