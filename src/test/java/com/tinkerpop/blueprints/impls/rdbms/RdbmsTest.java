// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.tinkerpop.blueprints.Direction.OUT;
import static com.google.common.collect.Lists.newArrayList;
import static com.tinkerpop.blueprints.Direction.IN;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.impls.mem.MemGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.MetaGraph;

import lombok.extern.slf4j.Slf4j;

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

    public void testVertexTestSuite() throws Exception {

        for (int i = 0; i < 3; i++) {
            Vertex v = graph_.addVertex(null);
            log.info("adding vertex {} {}", i, v);
        }

        for (int i = 1; i <= 5; i+=2) {
            Vertex v = graph_.getVertex(Integer.valueOf(i));
            log.info("got vertex when looking for {}: {}", Integer.valueOf(i), v);
        }

        Vertex v0 = graph_.getVertex(Long.valueOf(7000));
        log.info("got vertex when looking for 7000: {}", v0);

        log.info("all vertices...");
        for (Vertex v: graph_.getVertices()) {
            log.info("found vertex {}", v);
        }
        //((MetaGraph<RdbmsGraph>)graph_).getRawGraph().dump();

        log.info("=============== testing add/remove ===============");
        Vertex v1 = graph_.addVertex(null);
        log.info("added vertex null {}", v1);
        for (Vertex v: Iterables.limit(Lists.reverse(newArrayList(graph_.getVertices())), 2) ) {
            log.info("found vertex {}", v);
        }
        log.info("removing vertex {}", v1);
        graph_.removeVertex(v1);
        for (Vertex v: Iterables.limit(Lists.reverse(newArrayList(graph_.getVertices())), 2) ) {
            log.info("found vertex {}", v);
        }
        log.info("=============== testing add/remove ===============");

        Vertex v3 = graph_.getVertex(Long.valueOf(1));
        log.info("got vertex when looking for 1: {}", v3);
        v3.setProperty("sam", "i am43");
        v3.setProperty("String", "string");
        v3.setProperty("Long", Long.valueOf(17));
        v3.setProperty("Integer", Integer.valueOf(18));
        
        String s = v3.getProperty("sam");
        log.info("returned property: {}", s);

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
    @Override
    protected void setUp() throws Exception {
        graph_ = generateGraph();
        log.info("setUp graph '{}'", graph_);
    };
    // =================================
    @Override
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
