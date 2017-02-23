// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Date;

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
        final Graph graph = generateGraph();
        final Vertex vertexA = graph.addVertex(null);
        final Vertex vertexB = graph.addVertex(null);
        final Edge edge = graph.addEdge(null, vertexA, vertexB, "knows");
        vertexA.setProperty("keyDate", new Date());
//        trySetProperty(vertexA, "keyDate", new Date(), graph.getFeatures().supportsSerializableObjectProperty);
//        trySetProperty(edge, "keyDate", new Date(), graph.getFeatures().supportsSerializableObjectProperty);

//        this.stopWatch();
//        doTestSuite(new GraphTestSuite(this));
//        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    // =================================
    public Graph generateGraph() {
        if (null != graph_)
            graph_.shutdown();
        URL u = getClass().getClassLoader().getResource(GRAPH_PROPERTIES);
        graph_ =  GraphFactory.open(u.getFile());
        clearGraph();
        return graph_;
    }
    @Override
    public Graph generateGraph(String graphDirectoryName) {
        if (null != graph_)
            graph_.shutdown();
        URL u = getClass().getClassLoader().getResource(GRAPH_PROPERTIES);
        graph_ = GraphFactory.open(u.getFile());
        clearGraph();
        return graph_;
    }
    // =================================
    public void clearGraph() {
        @SuppressWarnings("unchecked")
        MetaGraph<RdbmsGraph> mg = (MetaGraph<RdbmsGraph>) graph_;
        RdbmsGraph rg = mg.getRawGraph();
        rg.clear();
        log.info("setUp graph '{}'", graph_);
    }
    // =================================
    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("RdbmsGraphTest");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                log.info("=============================");
                log.info("Test name {}", method.getName());

                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }
    // =================================
    Graph graph_ = null;
}
