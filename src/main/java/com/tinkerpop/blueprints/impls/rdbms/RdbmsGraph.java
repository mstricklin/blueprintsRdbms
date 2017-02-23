// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.tinkerpop.blueprints.impls.rdbms.PropertyStore.vertexPropertyStore;
import static com.tinkerpop.blueprints.impls.rdbms.PropertyStore.edgePropertyStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb.HsqldbDaoFactory;
import com.tinkerpop.blueprints.impls.rdbms.util.CovariantIterable;

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
        vps = null;
        eps = null;
    }
    // =================================
    public RdbmsGraph(final Configuration configuration) {
        if (log.isDebugEnabled()) {
            for (Entry<Object, Object> e : ConfigurationConverter.getProperties(configuration).entrySet()) {
                log.debug("{} => {}", e.getKey(), e.getValue());
            }
        }

        int cacheSize = configuration.getInt(CACHE_CONFIG, DEFAULT_CACHE_SIZE);
        vertexCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .maximumSize(cacheSize)
                .build(vertexLoader);
        edgeCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .maximumSize(cacheSize)
                .build(edgeLoader);

        dao = HsqldbDaoFactory.make(ConfigurationConverter.getProperties(configuration.subset(HIKARI_PREFIX)), this);
        // TODO: parameterize sizes of vertex & edge caches...?
        vps = vertexPropertyStore(dao.getPropertyDao(RdbmsElement.PropertyType.VERTEX));
        eps = edgePropertyStore(dao.getPropertyDao(RdbmsElement.PropertyType.EDGE));
    }
    // =================================
    private void dumpProperties(RdbmsElement e) {
        for (String k: e.getPropertyKeys())
            log.info("\t{} => {}", k, e.getProperty(k));
    }
    protected void dumpCache() {
        log.info("======= cache vertices =======");
        for (RdbmsVertex v: vertexCache.asMap().values()) {
            log.info(v.toString());
            dumpProperties(v);
            for (Edge e: v.getEdges(Direction.OUT))
                log.info("\t{}", e);
            for (Edge e: v.getEdges(Direction.IN))
                log.info("\t{}", e);
        }
        log.info("======= cache edges =======");
        for (RdbmsEdge e: edgeCache.asMap().values()) {
            log.info(e.toString());
            dumpProperties(e);
        }
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
        return cache(dao.getVertexDao().add());
    }
    // =================================
    @Override
    public Vertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        try {
            final Long longID;
            if (id instanceof Long)
                longID = (Long) id;
            else
                longID = Long.valueOf(id.toString());
            return vertexCache.get(longID);
        } catch (ExecutionException | InvalidCacheLoadException | ClassCastException e) {
            log.error("could not find vertex w id {}", id);
        }
        return null;
    }
    // =================================
    @Override
    public void removeVertex(Vertex vertex) {
        checkNotNull(vertex);
        for (Edge e : vertex.getEdges(Direction.BOTH))
            removeEdge(e);
        Long longId = (Long) vertex.getId();
        vertexCache.invalidate(longId);
        vps.remove(longId);
        dao.getVertexDao().remove(longId);
    }
    // =================================
    // covariant types suck in Java...
    @Override
    public Iterable<Vertex> getVertices() {
        return new CovariantIterable<Vertex>(dao.getVertexDao().list());
    }
    // =================================
    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        // TODO: this is likely to be a smaller list, so cache the return values
        Iterable<RdbmsVertex> vertices = dao.getVertexDao().list(key, value);
        for (RdbmsVertex v: vertices)
            vertexCache.put(v.rawId(), v);
        return new CovariantIterable<Vertex>(vertices);
    }
    // =================================
    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        RdbmsVertex oV = (RdbmsVertex)outVertex;
        RdbmsVertex iV = (RdbmsVertex)inVertex;
        RdbmsEdge edge = dao.getEdgeDao().add(oV.rawId(), iV.rawId(), label);
        oV.addOutEdge(edge.rawId());
        iV.addInEdge(edge.rawId());
        return cache(edge);
    }
    // =================================
    @Override
    public Edge getEdge(Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();
        try {
            final Long longID = (Long) id;
            return edgeCache.get(longID);
        } catch (ExecutionException | InvalidCacheLoadException | ClassCastException e) {
            log.error("could not find edge w id {}", id);
        }
        return null;
    }
    // =================================
    // covariant types suck in Java...
    @Override
    public Iterable<Edge> getEdges() {
        // can't assume all edges are in cache, so default to SoR
        return new CovariantIterable<Edge>(dao.getEdgeDao().list());
    }
    // =================================
    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        Iterable<RdbmsEdge> edges = dao.getEdgeDao().list(key, value);
        for (RdbmsEdge e: edges)
            edgeCache.put(e.rawId(), e);
        return new CovariantIterable<Edge>(edges);
    }
    // =================================
    Iterable<RdbmsEdge> getEdges(Long vertexID) {
        // can't assume all edges are in cache, so default to SoR
        Set<RdbmsEdge> edges = newHashSet(dao.getEdgeDao().list(vertexID));
        return cache(edges);
    }
    // =================================
    // TODO: the edge removes its own properties from the SoR, then we do it again here...
    @Override
    public void removeEdge(Edge edge) {
        checkNotNull(edge);
        RdbmsEdge e = (RdbmsEdge) edge;
        Long longId = e.rawId();
        e.getOutVertex().removeEdge(longId);
        e.getInVertex().removeEdge(longId);
        eps.remove(longId);
        dao.getEdgeDao().remove(longId);
        edgeCache.invalidate(longId);
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
    public void clear() {
        log.info("clearing everything from graph...");
        dao.clear();
        edgeCache.invalidateAll();
        vertexCache.invalidateAll();
        vps.clear();
        eps.clear();
    }
    public String toString() {
        return StringFactory.graphString(this,  "vertices:" + vertexCache.size() + " edges:" + edgeCache.size());
    }
    // =================================
    public PropertyStore vertexPropertyCache() {
        return vps;
    }
    public PropertyStore edgePropertyCache() {
        return eps;
    }
    // =================================
    private RdbmsVertex cache(RdbmsVertex v) {
        vertexCache.put(v.rawId(), v);
        return v;
    }
    private RdbmsEdge cache(RdbmsEdge e) {
        edgeCache.put(e.rawId(), e);
        return e;
    }
    private Iterable<RdbmsEdge> cache(Iterable<RdbmsEdge> it) {
        for (RdbmsEdge e: it)
            cache(e);
        return it;
    }
    // =================================
    private CacheLoader<Long, RdbmsVertex> vertexLoader = new CacheLoader<Long, RdbmsVertex>() {
        @Override
        public RdbmsVertex load(Long id) throws Exception {
            log.info("using cacheLoader for vertex {}", id);
            return dao.getVertexDao().get(id);
        }
    };
    private CacheLoader<Long, RdbmsEdge> edgeLoader = new CacheLoader<Long, RdbmsEdge>() {
        @Override
        public RdbmsEdge load(Long id) throws Exception {
            log.info("using cacheLoader for edge {}", id);
            return dao.getEdgeDao().get(id);
        }
    };
    // =================================
    private final DaoFactory dao;

    private final LoadingCache<Long, RdbmsVertex> vertexCache;
    private final LoadingCache<Long, RdbmsEdge> edgeCache;

    private final PropertyStore vps;
    private final PropertyStore eps;
}
