// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;


import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RdbmsEdge extends RdbmsElement implements Edge {

    // =================================
    public RdbmsEdge(final long edgeID, final long outID, final long inID, final String label, final RdbmsGraph graph) {
        super(graph.edgePropertyCache(), edgeID, graph);
        this.outVertexId = outID;
        this.inVertexId = inID;
        this.label = label;
    }
    // =================================
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return getOutVertex();
        if (direction.equals(Direction.IN))
            return getInVertex();
        throw ExceptionFactory.bothIsNotSupported();
    }
    // =================================
    RdbmsVertex getOutVertex() {
        return (RdbmsVertex)graph.getVertex(outVertexId);
    }
    RdbmsVertex getInVertex() {
        return (RdbmsVertex)graph.getVertex(inVertexId);
    }
    // =================================
    @Override
    public String getLabel() {
        return label;
    }
    // =================================
    @Override
    public void remove() {
        getGraph().removeEdge(this);
    }
    // =================================
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
    // =================================
    private final long outVertexId;
    private final long inVertexId;

    private final String label;

}
