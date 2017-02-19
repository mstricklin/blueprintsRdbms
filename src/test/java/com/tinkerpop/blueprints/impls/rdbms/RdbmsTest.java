// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;

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
        for (Vertex v : g.getVertices())
            log.info("Vertex {}", v);
        for (Edge e : g.getEdges())
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
    public void testGetNonexistentElements() throws Exception {
        assertNull(graph_.getVertex(Integer.MAX_VALUE));
        assertNull(graph_.getEdge(Integer.MAX_VALUE));
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
    // =================================
    @Test
    public void testAddGetEdge() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        assertNotNull(vA);
        assertNotNull(vB);
        Edge e = graph_.addEdge(null, vA, vB, "test-edge");
        assertNotNull(graph_.getEdge(e.getId()));
        assertEquals("test-edge", e.getLabel());

        assertEquals(vA, e.getVertex(Direction.OUT));
        assertEquals(vB, e.getVertex(Direction.IN));

        assertThat(vA.getEdges(Direction.OUT), hasItem(e));
        assertThat(vB.getEdges(Direction.IN), hasItem(e));
    }
    // =================================
    @Test
    public void testAddEdgeFromVertex() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        assertNotNull(vA);
        assertNotNull(vB);
        Edge e = vA.addEdge("test-edge", vB);
        assertNotNull(e);
        assertNotNull(graph_.getEdge(e.getId()));
        assertEquals("test-edge", e.getLabel());

        assertEquals(vA, e.getVertex(Direction.OUT));
        assertEquals(vB, e.getVertex(Direction.IN));

        assertThat(vA.getEdges(Direction.OUT), hasItem(e));
        assertThat(vB.getEdges(Direction.IN), hasItem(e));
    }
    // =================================
    @Test
    public void testRemoveEdge() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        assertNotNull(vA);
        assertNotNull(vB);
        Edge e = graph_.addEdge(null, vA, vB, "test-edge");
        assertNotNull(e);
        assertThat(vA.getEdges(Direction.OUT), hasItem(e));

        graph_.removeEdge(e);
        assertNull(graph_.getEdge(e.getId()));

        assertThat(vA.getEdges(Direction.OUT), not(hasItem(e)));
        assertThat(vB.getEdges(Direction.IN), not(hasItem(e)));
    }
    // =================================
    @Test
    public void testSelfRemoveEdge() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        assertNotNull(vA);
        assertNotNull(vB);
        Edge e = graph_.addEdge(null, vA, vB, "test-edge");
        assertNotNull(e);
        e.remove();
        graph_.removeEdge(e);
        assertNull(graph_.getEdge(e.getId()));
        assertThat(vA.getEdges(Direction.OUT), not(hasItem(e)));
        assertThat(vB.getEdges(Direction.IN), not(hasItem(e)));
    }
    // =================================
    @Test
    public void testVertexIteration() throws Exception {
        List<Vertex> vL = newArrayList();
        for (int i=0; i<5; ++i) {
            vL.add(graph_.addVertex(null));
        }
        List<Vertex> gL = newArrayList( graph_.getVertices() );
        assertEquals(vL, gL);
    }
    // =================================
    @Test
    public void testEdgeIteration() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        List<Edge> eL = newArrayList();
        for (int i=0; i<5; ++i) {
            eL.add( graph_.addEdge(null, vA, vB, "edge-"+i) );
        }
        List<Edge> gL = newArrayList( graph_.getEdges() );
        assertEquals(eL, gL);
    }
    // =================================
    @Test
    public void testVertexEdgeDirections() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        Edge e0 = vA.addEdge("edge0", vB);
        Edge e1 = vA.addEdge("edge1", vB);

        Collection<Edge> e;
        e = newArrayList( vA.getEdges(Direction.OUT) );
        assertThat(e, hasItem(e0));
        assertThat(e, hasItem(e1));
        e = newArrayList( vA.getEdges(Direction.IN) );
        assertTrue(e.isEmpty());

        e = newArrayList( vB.getEdges(Direction.IN) );
        assertThat(e, hasItem(e0));
        assertThat(e, hasItem(e1));
        e = newArrayList( vB.getEdges(Direction.OUT) );
        assertTrue(e.isEmpty());
    }
    // =================================
    @Test
    public void testVertexEdgeLabels() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        Edge e0 = vA.addEdge("edge0", vB);
        Edge e1 = vA.addEdge("edge1", vB);

        Collection<Edge> e;
        e = newArrayList( vA.getEdges(Direction.OUT, "edge0") );
        assertThat(e, hasItem(e0));
        assertThat(e, not( hasItem(e1) ));
        e = newArrayList( vA.getEdges(Direction.IN, "edge0") );
        assertTrue(e.isEmpty());

        e = newArrayList( vB.getEdges(Direction.IN, "edge0") );
        assertThat(e, hasItem(e0));
        assertThat(e, not( hasItem(e1) ));
        e = newArrayList( vB.getEdges(Direction.OUT, "edge0") );
        assertTrue(e.isEmpty());
    }
    // =================================
    @Test
    public void testVertexByProperties() throws Exception {
        Vertex vA = graph_.addVertex(null);
        Vertex vB = graph_.addVertex(null);
        vA.setProperty("String", "aaa");
        vB.setProperty("String", "bbb");
        for (Vertex v: graph_.getVertices()) {
            log.info(v.toString());
            for (String k: v.getPropertyKeys())
                log.info("\t{} => {}", k, v.getProperty(k));
        }
        log.info("=====");
        for (Vertex v: graph_.getVertices("String", "aaa")) {
            log.info(v.toString());
            for (String k: v.getPropertyKeys())
                log.info("\t{} => {}", k, v.getProperty(k));
        }
        log.info("=====");
        assertTrue(true);
    }

    // X add vertex
    // X get vertex
    // get vertices by key-value
    // X remove vertex (do edges get removed too?)
    // X self-remove vertex
    // X list vertex edges by label
    // X list vertex edges by direction
    // get neighbor vertices by vertex

    // X add edge
    // X add edge by vertex
    // X get edge
    // get edge by key-value
    // X remove edge
    // X iterate vertices
    // X iterate edges
    // iterate vertices by property
    // iterate edges by property

    // test persistence !!!
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


        for (int i = 1; i <= 5; i += 2) {
            Vertex v = graph_.getVertex(Integer.valueOf(i));
            log.info("got vertex when looking for {}: {}", Integer.valueOf(i), v);
        }

        Vertex v7000 = graph_.getVertex(Long.valueOf(7000));
        log.info("got vertex when looking for 7000: {}", v0);
    }
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
    private static int SAMPLE_SZ = 100;
    @Test
    public void testPopulate() throws Exception {
        Vertex v0 = graph_.addVertex(null);
        Vertex last = v0;
        for (int i = 0; i < SAMPLE_SZ; i++) {
            Vertex v = graph_.addVertex(null);
            v.setProperty("a"+i, "aaa");
            v.setProperty("b"+i, i);
            Edge e0 = graph_.addEdge (null, last, v, "edge-"+i);
            e0.setProperty("e0-"+i, "aaa");
            Edge e1 = graph_.addEdge(null, v0, v, "root-edge-"+i);
            e1.setProperty("e1-"+i, "aaa");
            last = v;
            log.info("added vertex {} {}", i, v);
        }
        assertTrue(true);
    }


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

        @SuppressWarnings("unchecked")
        MetaGraph<RdbmsGraph> mg = (MetaGraph<RdbmsGraph>) graph_;
        RdbmsGraph rg = mg.getRawGraph();
        rg.clear();
        log.info("setUp graph '{}'", graph_);
    }
    // =================================
    @After
    protected void tearDown() throws Exception {
        graph_.shutdown();
        // remove stored graph...?
        graph_ = null;
    }
    // =================================
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
