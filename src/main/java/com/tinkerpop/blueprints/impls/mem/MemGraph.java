// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;

import lombok.Data;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemGraph implements IndexableGraph, KeyIndexableGraph { // ,
                                                                                           // MetaGraph<GraphDatabaseService>
                                                                                           // {
    private static final Features FEATURES = new Features();

    static {
        // TODO: revisit this...
        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = false; // true
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = false;  //true
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }


    // =================================
    public MemGraph() {
        log.info("create MemGraph");
    }

    // =================================
    public MemGraph(final Configuration configuration) {
        dump(ConfigurationConverter.getProperties(configuration));
    }

    // =================================
    protected void dump(Properties p) {
        if (log.isDebugEnabled()) {
            log.debug("RdbmsGraph properties...");
            for (Entry<Object, Object> e : p.entrySet()) {
                log.debug("{} => {}", e.getKey(), e.getValue());
            }
        }
    }

    // =================================
    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    // =================================
    @Override
    public Vertex addVertex(Object id) {
        MemVertex v = new MemVertex(this);
        log.trace("addVertex {}", v);
        vertices_.put(v.getId(), v);
        return v;
    }

    // =================================
    @Override
    public Vertex getVertex(Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        try {
            final Long longID;
            if (id instanceof Long)
                longID = (Long) id;
            else
                longID = Long.valueOf(id.toString());
            log.trace("getVertex with id {}", longID);
            return vertices_.get(longID);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    // =================================
    @Override
    public void removeVertex(Vertex vertex) {
        // TODO: SQL
        log.trace("removing vertex {}", vertex);
        for (Edge e : newArrayList(vertex.getEdges(Direction.BOTH))) {
            log.trace("remove edge {}", e);
            removeEdge(e);
        }
        vertices_.remove(vertex.getId());
    }

    // =================================
    @Override
    public Iterable<Vertex> getVertices() {
        return new ArrayList<Vertex>(vertices_.values());
    }

    // =================================
    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return new PropertyFilteredIterable<Vertex>(key, value, this.getVertices());
    }

    // =================================
    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        Edge e = new MemEdge(outVertex, inVertex, label, this);
        log.trace("addEdge {}", e);
        ((MemVertex)outVertex).addOutEdge(label, e);
        ((MemVertex)inVertex).addInEdge(label, e);
        edges_.put(e.getId(), e);
        return e;
    }

    // =================================
    @Override
    public Edge getEdge(Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();
        return edges_.get(id);
    }

    // =================================
    @Override
    public void removeEdge(Edge edge) {
        MemVertex outVertex = (MemVertex) edge.getVertex(Direction.OUT);
        outVertex.removeEdge(edge);
        MemVertex inVertex = (MemVertex) edge.getVertex(Direction.IN);
        inVertex.removeEdge(edge);

        edges_.remove(edge.getId());
    }

    // =================================
    @Override
    public Iterable<Edge> getEdges() {
        return new ArrayList<Edge>(edges_.values());
    }

    // =================================
    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return new PropertyFilteredIterable<Edge>(key, value, this.getEdges());
    }

    // =================================
    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    // =================================
    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        log.info("dropKeyIndex key {}", key);
        if (Vertex.class.isAssignableFrom(elementClass)) {
            vertexKeyIndex_.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            edgeKeyIndex_.dropKeyIndex(key);
        }
    }
    // =================================
    // a "KeyIndex" instructs an optimization on elements properties, to speed
    // searches on said properties. We're already going to index on key/values
    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        log.info("createKeyIndex key {}", key);
        if (Vertex.class.isAssignableFrom(elementClass)) {
            vertexKeyIndex_.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            edgeKeyIndex_.createKeyIndex(key);
        }

    }
    // =================================
    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        log.info("getIndexedKeys");
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexKeyIndex_.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeKeyIndex_.getIndexedKeys();
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }
    // =================================

    @Override
    public <T extends Element> Index<T> createIndex(String indexName, Class<T> indexClass,
            Parameter... indexParameters) {
        log.info("create index {}", indexName);
        if (indices_.containsRow(indexName))
            throw ExceptionFactory.indexAlreadyExists(indexName);

        final MemIndex<T> index = new MemIndex<T>(indexName, indexClass);
        indices_.put(indexName, indexClass, index);

        return index;
    }
    // =================================
    // An Index is orthogonal to element properties. They are are independent mapping schema
    // to find vertices and edges.
    @SuppressWarnings("unchecked")
    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        return (Index<T>)indices_.get(indexName, indexClass);
    }
    // =================================
    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        return new ArrayList<Index<? extends Element>>(indices_.values());
    }
    // =================================
    @Override
    public void dropIndex(String indexName) {
        // remove all possible mappings
        indices_.remove(indexName, Vertex.class);
        indices_.remove(indexName, Edge.class);
    }
    // =================================
    @Override
    public void shutdown() {
    }
    // =================================
    @Override
    public String toString() {
        //return StringFactory.graphString(this, cpm_.toString());
        return StringFactory.graphString(this, "vertices:" + vertices_.size() + " edges:" + edges_.size());
    }
    // =================================
    protected static class MemKeyIndex<T extends MemElement> extends MemIndex<T> {

        MemKeyIndex(Class<T> indexClass, MemGraph g) {
            super(null, indexClass);
            graph_ = g;
        }
        // =================================
        public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
            if (this.keys_.contains(key)) {
                if (oldValue != null)
                    this.remove(key, oldValue, element);
                this.put(key, newValue, element);
            }
        }
        // =================================
        public void autoRemove(final String key, final Object oldValue, final T element) {
            if (this.keys_.contains(key)) {
                this.remove(key, oldValue, element);
            }
        }
        // =================================
        @SuppressWarnings("unchecked")
        public void createKeyIndex(final String key) {
            this.keys_.add(key);
            // re-index
            log.error("re-index on key-index creation");
            if (Vertex.class.isAssignableFrom(getIndexClass())) {
                for (Vertex v: graph_.getVertices()) {
                    put(key, v.getProperty(key), (T)v);
                }
            } else if (Edge.class.isAssignableFrom(getIndexClass())) {
                for (Edge e: graph_.getEdges()) {
                    put(key, e.getProperty(key), (T)e);
                }
            }
        }
        // =================================
        public void dropKeyIndex(final String key) {
            this.keys_.remove(key);
            remove(key);

        }
        // =================================
        public Set<String> getIndexedKeys() {
            return newHashSet(keys_);
        }
        private final Set<String> keys_ = newHashSet();
        private final MemGraph graph_;
    }



    private final Map<Object, Vertex> vertices_ = newHashMap();
    private final Map<Object, Edge> edges_ = newHashMap();

    protected Table<String, Class<? extends Element>, MemIndex<? extends Element>> indices_ = HashBasedTable.create();

    protected MemKeyIndex<MemVertex> vertexKeyIndex_ = new MemKeyIndex<MemVertex>(MemVertex.class, this);
    protected MemKeyIndex<MemEdge>   edgeKeyIndex_   = new MemKeyIndex<MemEdge>(MemEdge.class, this);


//    private final Map<String, Index<Vertex>> indices_ = newHashMap();
}
