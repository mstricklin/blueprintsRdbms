// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;

public class VertexIndex implements Index<Vertex> {

    @Override
    public String getIndexName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<Vertex> getIndexClass() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(String key, Object value, Vertex element) {
        // TODO Auto-generated method stub

    }

    @Override
    public CloseableIterable<Vertex> get(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CloseableIterable<Vertex> query(String key, Object query) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long count(String key, Object value) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void remove(String key, Object value, Vertex element) {
        // TODO Auto-generated method stub

    }

}
