// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

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
public class FramedMemGraphTest extends TestCase {

    @Test
    public void test() {
        Vertex v0 = framedGraph.addVertex("72");
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

        FramedGraphFactory factory = new FramedGraphFactory();
        Graph g = new MemGraph();
        framedGraph = factory.create(g);

        log.info("setUp graph '{}'", g);
        log.info("setUp FramedGraph '{}'", framedGraph);
    };
    FramedGraph<Graph> framedGraph = null;

}
