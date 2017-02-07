// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import java.io.File;
import java.lang.reflect.Method;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Neo4jGraphTest extends GraphTest {

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
//        doTestSuite(new VertexTestSuite(this));
//        VertexTestSuite vts = new VertexTestSuite(this);
//        vts.testVertexEquality();

        Graph graph = generateGraph();
        Vertex v = graph.addVertex(null);
        Vertex u = graph.getVertex(v.getId());
        assertNotNull(u);
        log.info("retrieved vertex: {}", u);
        //u.setProperty(null, -1);
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }
    // =================================
    @Override
    public Graph generateGraph() {
        if (null != ng_)
            ng_.shutdown();
        deleteDirectory(new File(NEO4J_DIR));
        System.out.println("generateGraph explicit dir");
        ng_ = new Neo4jGraph(NEO4J_DIR);
        return ng_;
    }
    // =================================
    @Override
    public Graph generateGraph(String graphDirectoryName) {
        if (null != ng_)
            ng_.shutdown();
        deleteDirectory(new File(graphDirectoryName));
        System.out.println("generateGraph explicit dir");
        ng_ = new Neo4jGraph(graphDirectoryName);
        return ng_;

    }
    // =================================
    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("Neo4jGraphTest");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }
    // =================================
    private static final String NEO4J_DIR = "work";
    Neo4jGraph ng_;
}
