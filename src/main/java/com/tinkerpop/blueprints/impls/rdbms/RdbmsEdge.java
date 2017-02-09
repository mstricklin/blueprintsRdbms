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
    public RdbmsEdge(final int vertexID, final int outID, final int inID, final String label, final RdbmsGraph graph) {
        // TODO: fix coherence here...
        super(vertexID, graph);
        this.outVertex = new RdbmsVertex(outID, graph);
        this.inVertex = new RdbmsVertex(inID, graph);
        this.label = label;
    }
    // =================================
    public RdbmsEdge(final int vertexID, final Vertex outVertex, final Vertex inVertex, final String label, final RdbmsGraph graph) {
        super(vertexID, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
    }
    // =================================
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return outVertex;
        if (direction.equals(Direction.IN))
            return inVertex;
        throw ExceptionFactory.bothIsNotSupported();
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
    private final Vertex outVertex;
    private final Vertex inVertex;
    private final String label;

}
