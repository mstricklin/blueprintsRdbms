// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.neo4j;

import java.io.File;
import java.lang.reflect.Method;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
//import com.tinkerpop.blueprints.pgm.AutomaticIndexTestSuite;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Neo4jGraphTest extends GraphTest {

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
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

//    public void testAutomaticIndexTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new AutomaticIndexTestSuite(this));
//        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
//    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
//        doTestSuite(new TransactionalGraphTestSuite(this));
        (new GraphTestSuite(this)).testGraphDataPersists();
        printTestPerformance("TransactionalGraphTestSuite", this.stopWatch());
    }

//    public void testGraphMLReaderTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new GraphMLReaderTestSuite(this));
//        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
//    }

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
