// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.MetaGraph;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class RdbmsTest extends GraphTest {
    private static final String GRAPH_PROPERTIES = "graph.properties";
    // =================================
    private void dump(Graph g) {
        log.info("====== Dump ========");
        for (Vertex v: g.getVertices())
            log.info("Vertex {}", v);
        for (Edge e: g.getEdges())
            log.info("Edge {}", e);
    }
    // =================================
    @Test
    public void testAddGetVertex() throws Exception {
        Vertex v0 = graph_.addVertex(null);
        assertNotNull(v0);
        Vertex v0a = graph_.getVertex(v0.getId());
        assertEquals(v0, v0a);
    }
    // =================================
    @Test
    public void testRemoveVertex() throws Exception {
        Vertex v0 = graph_.addVertex(null);
        assertNotNull(v0);
        graph_.removeVertex(v0);
        assertNull(graph_.getVertex(v0.getId()));

        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        Edge e = graph_.addEdge(null, vA, vB, "test-edge");
        assertNotNull(graph_.getVertex(vA.getId()));
        assertNotNull(graph_.getVertex(vB.getId()));
        assertNotNull(graph_.getEdge(e.getId()));

        graph_.removeVertex(vA);
        assertNull(graph_.getVertex(vA.getId()));
        assertNull(graph_.getEdge(e.getId()));
        assertNotNull(graph_.getVertex(vB.getId()));
    }
    // =================================
    @Test
    public void testSelfRemoveVertex() throws Exception {
        Vertex v0 = graph_.addVertex(null);
        assertNotNull(v0);
        v0.remove();
        assertNull(graph_.getVertex(v0.getId()));

        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        Edge e = graph_.addEdge(null, vA, vB, "test-edge");
        assertNotNull(graph_.getVertex(vA.getId()));
        assertNotNull(graph_.getVertex(vB.getId()));
        assertNotNull(graph_.getEdge(e.getId()));

        vA.remove();
        assertNull(graph_.getVertex(vA.getId()));
        assertNull(graph_.getEdge(e.getId()));
        assertNotNull(graph_.getVertex(vB.getId()));
    }
    // add vertex
    // get vertex
    // get vertices by key-value
    // remove vertex (do edges get removed too?)
    // self-remove vertex
    // list vertex edges by label, direction
    // get neighbor vertices by vertex

    // add edge
    // add edge by vertex
    // get edge
    // get edge by key-value
    // remove edge
    // iterate vertices
    // iterate edges
    // iterate vertices by property
    // iterate edges by property
    // test persistence
    // transactions
    @Test
    public void testVertexProperties() throws Exception {
        Vertex v = graph_.addVertex(null);
        log.info("new vertex {}", v);
        assertNotNull(v);
        v.setProperty("String", "aaa");
        v.setProperty("Long", 17L);
        v.setProperty("Boolean", true);
        Set<String> keys = v.getPropertyKeys();
        assertThat(keys, hasItem("String"));
        assertEquals("aaa", v.getProperty("String"));
        assertThat(keys, hasItem("Long"));
        assertEquals(17L, v.getProperty("Long"));
        assertThat(keys, hasItem("Boolean"));
        assertEquals(Boolean.TRUE, v.getProperty("Boolean"));

        // remove property
        v.removeProperty("Boolean");
        keys = v.getPropertyKeys();
        assertThat(keys, not(hasItem("Boolean")));
        assertNull(v.getProperty("Boolean"));

        // overwrite property
        keys = v.getPropertyKeys();
        assertThat(keys, hasItem("String"));
        assertEquals("aaa", v.getProperty("String"));
        v.setProperty("String", "bbb");
        assertEquals("bbb", v.getProperty("String"));
    }
    // =================================
    @Test
    public void testEdgeProperties() throws Exception {
        Vertex v0 = graph_.addVertex(null);
        Vertex v1 = graph_.addVertex(null);
        Edge e = graph_.addEdge(null, v0, v1, "test-vertex");
        log.info("new edge {}", e);
        assertNotNull(e);
        assertEquals("test-vertex", e.getLabel());
        e.setProperty("String", "aaa");
        e.setProperty("Long", 17L);
        e.setProperty("Boolean", true);
        Set<String> keys = e.getPropertyKeys();
        assertThat(keys, hasItem("String"));
        assertEquals("aaa", e.getProperty("String"));
        assertThat(keys, hasItem("Long"));
        assertEquals(17L, e.getProperty("Long"));
        assertThat(keys, hasItem("Boolean"));
        assertEquals(Boolean.TRUE, e.getProperty("Boolean"));

        // remove property
        e.removeProperty("Boolean");
        keys = e.getPropertyKeys();
        assertThat(keys, not(hasItem("Boolean")));
        assertNull(e.getProperty("Boolean"));

        // overwrite property
        keys = e.getPropertyKeys();
        assertThat(keys, hasItem("String"));
        assertEquals("aaa", e.getProperty("String"));
        e.setProperty("String", "bbb");
        assertEquals("bbb", e.getProperty("String"));
    }
    // =================================
    @Test
    public void testVertexTestSuite() throws Exception {

        for (int i = 0; i < 3; i++) {
            Vertex v = graph_.addVertex(null);
            log.info("added vertex {} {}", i, v);
        }
        Vertex v0 = graph_.getVertices().iterator().next();
        assertNotNull(v0);
        v0.setProperty("String", "aaa");
        v0.setProperty("Long", 17L);
        v0.setProperty("Boolean", true);
        Set<String> keys = v0.getPropertyKeys();
        assertThat(keys, hasItem("String"));
        assertEquals("aaa", v0.getProperty("String"));
        assertThat(keys, hasItem("Long"));
        assertEquals(17L, v0.getProperty("Long"));
        assertThat(keys, hasItem("Boolean"));
        assertEquals(Boolean.TRUE, v0.getProperty("Boolean"));


        for (int i = 1; i <= 5; i+=2) {
            Vertex v = graph_.getVertex(Integer.valueOf(i));
            log.info("got vertex when looking for {}: {}", Integer.valueOf(i), v);
        }

        Vertex v7000 = graph_.getVertex(Long.valueOf(7000));
        log.info("got vertex when looking for 7000: {}", v0);

//        log.info("all vertices...");
//        for (Vertex v: graph_.getVertices()) {
//            log.info("found vertex {}", v);
//        }
//        //((MetaGraph<RdbmsGraph>)graph_).getRawGraph().dump();
//
//        log.info("=============== testing add/remove ===============");
//        Vertex v1 = graph_.addVertex(null);
//        log.info("added vertex null {}", v1);
//        for (Vertex v: Iterables.limit(Lists.reverse(newArrayList(graph_.getVertices())), 2) ) {
//            log.info("found vertex {}", v);
//        }
//        log.info("removing vertex {}", v1);
//        graph_.removeVertex(v1);
//        for (Vertex v: Iterables.limit(Lists.reverse(newArrayList(graph_.getVertices())), 2) ) {
//            log.info("found vertex {}", v);
//        }
//        log.info("=============== testing properties ===============");
//
//        Vertex v3 = graph_.getVertex(Long.valueOf(1));
//        log.info("got vertex when looking for 1: {}", v3);
//
//        String s = v3.getProperty("sam");
//        log.info("returned property: {}", s);
//
//        Long l = v3.getProperty("Long");
//        log.info("returned property: {}", l);
//        Object o = v3.getProperty("Long");
//        log.info("returned property type: {}", o.getClass().getName());
//
//        v3.setProperty("sam", "i am43");
//        v3.setProperty("String", "string");
//        v3.setProperty("Long", Long.valueOf(17));
//        v3.setProperty("Integer", Integer.valueOf(18));
//
//        String s0 = v3.getProperty("sam");
//        log.info("returned property: {}", s0);

//
//        this.stopWatch();
//        doTestSuite(new VertexTestSuite(this));
//        printTestPerformance("VertexTestSuite", this.stopWatch());
    }
//    public void testEdgeTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new EdgeTestSuite(this));
//        printTestPerformance("EdgeTestSuite", this.stopWatch());
//    }
//
//    public void testGraphTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new GraphTestSuite(this));
//        printTestPerformance("GraphTestSuite", this.stopWatch());
//    }
//    public void testKeyIndexableGraphTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new KeyIndexableGraphTestSuite(this));
//        printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
//    }
//    public void testIndexableGraphTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new IndexableGraphTestSuite(this));
//        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
//    }



    // =================================
    @Override
    public Graph generateGraph() {
        URL u = getClass().getClassLoader().getResource(GRAPH_PROPERTIES);
        return GraphFactory.open(u.getFile());
    }
    // =================================
    @Override
    public Graph generateGraph(String graphDirectoryName) {
        log.error("can't generate a graph from a dir name...?");
        return null;
    }
    // =================================
    @Before
    protected void setUp() throws Exception {
        graph_ = generateGraph();
        MetaGraph<RdbmsGraph> mg = (MetaGraph<RdbmsGraph>)(graph_);
        RdbmsGraph rg = mg.getRawGraph();
        rg.clear();
        log.info("setUp graph '{}'", graph_);
    };
    // =================================
    @After
    protected void tearDown() throws Exception {
        graph_.shutdown();
        // remove stored graph...?
        graph_ = null;
    };

    Graph graph_ = null;
    // =================================
    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testRdbmsGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }

    }
}
