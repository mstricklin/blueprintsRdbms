// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb.HsqldbDaoFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RdbmsGraph implements TransactionalGraph, IndexableGraph, KeyIndexableGraph, MetaGraph<RdbmsGraph> {

    private static final int DEFAULT_CACHE_SIZE = 1000;
    private static final String CACHE_CONFIG = "blueprints.rdbms.cacheSize";
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
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = false; // true
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }

    private final String HIKARI_PREFIX = "blueprints.rdbms.hikari";

    // =================================
    public RdbmsGraph() {
        dao = null;
        vertexCache = null;
        edgeCache = null;
    }

    // =================================
    public RdbmsGraph(final Configuration configuration) {
        if (log.isDebugEnabled()) {
            for (Entry<Object, Object> e : ConfigurationConverter.getProperties(configuration).entrySet()) {
                log.debug("{} => {}", e.getKey(), e.getValue());
            }
        }

        int cacheSize = configuration.getInt(CACHE_CONFIG, DEFAULT_CACHE_SIZE);
        vertexCache = CacheBuilder.newBuilder().concurrencyLevel(4).maximumSize(cacheSize).build(vertexLoader);
        edgeCache = CacheBuilder.newBuilder().concurrencyLevel(4).maximumSize(cacheSize).build(edgeLoader);

        dao = HsqldbDaoFactory.make(ConfigurationConverter.getProperties(configuration.subset(HIKARI_PREFIX)), this);
    }

    // =================================
    protected void dump() {
        if (log.isDebugEnabled()) {
            log.debug("RdbmsGraph vertices...");
            for (Vertex v : getVertices()) {
                log.debug(v.toString());
            }
            // log.debug("RdbmsGraph edges...");
            // for (Edge e: getEdges()) {
            // log.debug(e.toString());
            // }
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
        RdbmsVertex v = dao.getVertexDao().add();
        vertexCache.put(v.getId(), v);
        return v;
    }
    // =================================
    @Override
    public Vertex getVertex(final Object id) {
        checkNotNull(id);
        try {
            return vertexCache.get(id);
        } catch (ExecutionException | InvalidCacheLoadException e) {
            log.error("could not find vertex w id {}", id);
        }
        return null;
    }

    // =================================
    @Override
    public void removeVertex(Vertex vertex) {
        checkNotNull(vertex);
        synchronized (this) {
            vertexCache.invalidate(vertex.getId());
            dao.getVertexDao().remove(vertex.getId());
        }
    }
    // =================================
    // covariant types suck in Java...
    // TODO: revisit this...can we be lazy?
    @Override
    public Iterable<Vertex> getVertices() {
        return ImmutableList.copyOf(dao.getVertexDao().list());
    }
    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return (Iterable<Vertex>) (dao.getVertexDao().list(key, value));
    }
    // =================================
    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        RdbmsEdge e = dao.getEdgeDao().add(outVertex, inVertex, label);
        edgeCache.put(e.getId(), e);
        return e;
    }
    // =================================
    @Override
    public Edge getEdge(Object id) {
        checkNotNull(id);
        try {
            return edgeCache.get(id);
        } catch (ExecutionException | InvalidCacheLoadException e) {
            log.error("could not find edge w id {}", id);
        }
        return null;
    }
    // =================================
    @Override
    public void removeEdge(Edge edge) {
        checkNotNull(edge);
        synchronized (this) {
            vertexCache.invalidate(edge.getId());
            dao.getEdgeDao().remove(edge.getId());
        }
    }
    // =================================
    // covariant types suck in Java...
    @Override
    public Iterable<Edge> getEdges() {
        return ImmutableList.copyOf(dao.getEdgeDao().list());
    }

    // =================================
    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return (Iterable<Edge>) (dao.getEdgeDao().list(key, value));
    }

    // =================================
    @Override
    public GraphQuery query() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Element> Index<T> createIndex(String indexName, Class<T> indexClass,
            Parameter... indexParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void dropIndex(String indexName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        dao.close();
    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback() {
        // TODO Auto-generated method stub

    }

    // =================================
    protected DaoFactory getDaoFactory() {
        return dao;
    }

    // =================================
    @Override
    public RdbmsGraph getRawGraph() {
        return this;
    }

    // =================================
    private CacheLoader<Object, RdbmsVertex> vertexLoader = new CacheLoader<Object, RdbmsVertex>() {
        @Override
        public RdbmsVertex load(Object key) throws Exception {
            log.info("using cacheLoader for vertex");
            return dao.getVertexDao().get(key);
        }
    };
    private CacheLoader<Object, RdbmsEdge> edgeLoader = new CacheLoader<Object, RdbmsEdge>() {
        @Override
        public RdbmsEdge load(Object key) throws Exception {
            log.info("using cacheLoader for edge");
            return dao.getEdgeDao().get(key);
        }
    };
    // =================================
    private final DaoFactory dao;

    private final LoadingCache<Object, RdbmsVertex> vertexCache;
    private final LoadingCache<Object, RdbmsEdge> edgeCache;

}
