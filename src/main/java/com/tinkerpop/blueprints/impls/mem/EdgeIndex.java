// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;

public class EdgeIndex implements Index<Edge> {

    @Override
    public String getIndexName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<Edge> getIndexClass() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(String key, Object value, Edge element) {
        // TODO Auto-generated method stub

    }

    @Override
    public CloseableIterable<Edge> get(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CloseableIterable<Edge> query(String key, Object query) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long count(String key, Object value) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void remove(String key, Object value, Edge element) {
        // TODO Auto-generated method stub

    }

}
