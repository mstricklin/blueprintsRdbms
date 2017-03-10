// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.neo4j;

import static org.junit.Assert.*;

import org.junit.Test;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.mem.MemGraph;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FramedNeo4jGraphTest extends TestCase {

    @Test
    public void test() {
        Vertex v0 = g_.addVertex("72");
        log.info("Vertex: {}", v0);
        log.info("Vertex: {}", v0.getId());
        for (String k: v0.getPropertyKeys()) {
            log.info("\t{} => {}", k, v0.getProperty(k));
        }
        assertTrue(true);
    }

    // =================================
    @Override
    protected void setUp() throws Exception {

        g_ = new Neo4jGraph(NEO4J_DIR);
        FramedGraphFactory factory = new FramedGraphFactory();
        framedGraph = factory.create((Graph)g_);

//        FramedGraphFactory factory = new FramedGraphFactory(); // make sure you reuse the factory when creating new framed graphs.
//        Graph g = new MemGraph();
//        framedGraph = factory.create(g);

        log.info("setUp graph '{}'", g_);
//        log.info("setUp FramedGraph '{}'", framedGraph);
    };
    private static final String NEO4J_DIR = "work";
    Neo4jGraph g_;
    FramedGraph<Graph> framedGraph = null;

}
