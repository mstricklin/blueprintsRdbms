// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.sql.DataSource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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

    private static final Features FEATURES = new Features();
    private static final int CACHE_SIZE = 1000;

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

    private final String HIKARI_PREFIX = "blueprints.rdbms.hikari";
    private final String QUERY_PREFIX = "queries";
    private final String QUERY_FILE_PROPERTY = "blueprints.rdbms.queriesFile";
    private final String DEFAULT_QUERY_FILE = "queries.properties";
    private final Properties queries_ = new Properties();
    // =================================
    public RdbmsGraph() {
        cpm = null;
        ds = null;
        dao = null;
    }

    // =================================
    public RdbmsGraph(final Configuration configuration) {
        if (log.isDebugEnabled()) {
            for (Entry<Object, Object> e : ConfigurationConverter.getProperties(configuration).entrySet()) {
                log.debug("{} => {}", e.getKey(), e.getValue());
            }
        }

        dao = HsqldbDaoFactory.make(
                ConfigurationConverter.getProperties(configuration.subset(HIKARI_PREFIX)),
                this);

        Configuration hikariConfig = configuration.subset(HIKARI_PREFIX);
        cpm = new ConnectionPoolManager(ConfigurationConverter.getProperties(hikariConfig));
        ds = cpm.getDataSource();
        log.info("CPM: {}", cpm);

        new SchemaVersionManager(cpm.getDataSource()).migrate();

//        Configuration queries = configuration.subset(QUERY_PREFIX);

        String queryFile = configuration.getString(QUERY_FILE_PROPERTY, DEFAULT_QUERY_FILE);
        log.info("====================");
        log.info("Query file: {}", queryFile);

        try {

          InputStream is = getClass().getClassLoader().getResourceAsStream(queryFile);
          queries_.load(is);
          log.info("Queries: {}", queries_);
          log.info("Queries: {}", queries_.getProperty("create.vertex", "foo"));


        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error("Error loading queries", e);
        }

    }
    // =================================
    protected void dump() {
        if (log.isDebugEnabled()) {
            log.debug("RdbmsGraph vertices...");
//            for (Vertex v: getVertices()) {
//                log.debug(v.toString());
//            }
//            log.debug("RdbmsGraph edges...");
//            for (Edge e: getEdges()) {
//                log.debug(e.toString());
//            }
            log.debug("RdbmsGraph queries...");
            for (Entry<Object, Object> e: queries_.entrySet()) {
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
    	RdbmsVertex v = dao.getVertexDao().add();
    	vertexCache.put(v.getId(), v);
    	return v;
    }
    // =================================
    private static <T> T firstNonNull(T a, T b) {
    	return a != null ? a : b;
    }
    // =================================
    @Override
    public Vertex getVertex(final Object id) {
    	checkNotNull(id);
    	RdbmsVertex v = vertexCache.getIfPresent(id);
    	if (null!=v)
    		return v;
        return dao.getVertexDao().get(id);
    }
    // =================================
    @Override
    public void removeVertex(Vertex vertex) {
        checkNotNull(vertex);
        vertexCache.invalidate(vertex.getId());
        dao.getVertexDao().remove(vertex.getId());
    }
    // =================================
    // covariant types suck in Java...
	@Override
    public Iterable<Vertex> getVertices() {
    	return ImmutableList.copyOf(dao.getVertexDao().list());
    }
    // =================================
    @SuppressWarnings("unchecked")
	@Override
    public Iterable<Vertex> getVertices(String key, Object value) {
    	return (Iterable<Vertex>)(dao.getVertexDao().list(key, value));
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
    	RdbmsEdge e = edgeCache.getIfPresent(id);
    	if (null!=e)
    		return e;
        return dao.getEdgeDao().get(id);
    }
    // =================================
    @Override
    public void removeEdge(Edge edge) {
    	checkNotNull(edge);
        vertexCache.invalidate(edge.getId());
        dao.getEdgeDao().remove(edge.getId());
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
    	return (Iterable<Edge>)(dao.getEdgeDao().list(key, value));
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
        try {
            if (null != cpm)
                cpm.close();
        } catch (SQLException e) {
            log.error("Error shutting down DB", e);
        }
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
    protected DaoFactory
    getDaoFactory() {
    	return dao;
    }
    // =================================
    protected Connection
    dbConn() throws SQLException {
        return ds.getConnection();
    }
    // =================================
    @Override
    public RdbmsGraph
    getRawGraph() {
        return this;
    }
    // =================================
    private final ConnectionPoolManager cpm;
    private final DataSource ds;
    private final DaoFactory dao;

    private final Cache<Object, RdbmsVertex> vertexCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();
    private final Cache<Object, RdbmsEdge>   edgeCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build();

}
