// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;

@Slf4j
public class RdbmsGraphTest extends GraphTest {
    private static final String GRAPH_PROPERTIES = "graph.properties";

//    public void testNeo4jBenchmarkTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new Neo4jBenchmarkTestSuite(this));
//        printTestPerformance("Neo4jBenchmarkTestSuite", this.stopWatch());
//    }


    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }
    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }
    // =================================
    public void testClear() {
        RdbmsGraph graph = (RdbmsGraph) this.generateGraph();
        graph.clear();
        this.stopWatch();
        for (int i = 0; i < 25; i++) {
            Vertex a = graph.addVertex(null);
            Vertex b = graph.addVertex(null);
            graph.addEdge(null, a, b, "knows");
        }
        printPerformance(graph.toString(), 75, "elements added", this.stopWatch());

        assertEquals(50, count(graph.getVertices()));
        assertEquals(25, count(graph.getEdges()));

        this.stopWatch();
        graph.clear();
        printPerformance(graph.toString(), 75, "elements deleted", this.stopWatch());

        assertEquals(0, count(graph.getVertices()));
        assertEquals(0, count(graph.getEdges()));

        graph.shutdown();
    }
    // =================================
    public void testShutdownStartManyTimes() {
        RdbmsGraph graph = (RdbmsGraph) this.generateGraph();
        graph.clear();
        for (int i = 0; i < 25; i++) {
            Vertex a = graph.addVertex(null);
            a.setProperty("name", "a" + UUID.randomUUID());
            Vertex b = graph.addVertex(null);
            b.setProperty("name", "b" + UUID.randomUUID());
            graph.addEdge(null, a, b, "knows").setProperty("weight", 1);
        }
        graph.shutdown();
        this.stopWatch();
        int iterations = 150;
        for (int i = 0; i < iterations; i++) {
            graph = (RdbmsGraph) this.generateGraph();
            assertEquals(50, count(graph.getVertices()));
            for (final Vertex v : graph.getVertices()) {
                assertTrue(v.getProperty("name").toString().startsWith("a") || v.getProperty("name").toString().startsWith("b"));
            }
            assertEquals(25, count(graph.getEdges()));
            for (final Edge e : graph.getEdges()) {
                assertEquals(e.getProperty("weight"), 1);
            }

            graph.shutdown();
        }
        printPerformance(graph.toString(), iterations, "iterations of shutdown and restart", this.stopWatch());
    }
    // =================================
    @Override
    public Graph generateGraph() {
        if (null != graph_)
            graph_.shutdown();
        URL u = getClass().getClassLoader().getResource(GRAPH_PROPERTIES);
        graph_ =  GraphFactory.open(u.getFile());
        return graph_;
    }
    @Override
    public Graph generateGraph(String graphDirectoryName) {
        if (null != graph_)
            graph_.shutdown();
        URL u = getClass().getClassLoader().getResource(GRAPH_PROPERTIES);
        graph_ = GraphFactory.open(u.getFile());
        return graph_;
    }
    public RdbmsGraph generateRdbmsGraph() {
        return (RdbmsGraph) this.generateGraph();
    }
    // =================================
    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("RdbmsGraphTest");
        generateRdbmsGraph().clear();
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                log.info("=============================");
                log.info("Test name {}", method.getName());

                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                    generateRdbmsGraph().clear();
                }
            }
        }
    }
    // =================================
    Graph graph_ = null;
}
