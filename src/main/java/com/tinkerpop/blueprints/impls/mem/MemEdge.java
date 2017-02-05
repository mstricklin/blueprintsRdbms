// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

public class MemEdge extends MemElement implements Edge {

    MemEdge(final Vertex outVertex, final Vertex inVertex, final String label, final MemGraph graph) {
        super(edgeID_++, graph);
        if (null == label)
            throw ExceptionFactory.edgeLabelCanNotBeNull();
        label_ = label;
        outVertex_ = outVertex;
        inVertex_ = inVertex;
    }
    // =================================
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return outVertex_;
        if (direction.equals(Direction.IN))
            return inVertex_;
        throw ExceptionFactory.bothIsNotSupported();
    }
    // =================================
    @Override
    public String getLabel() {
        return label_;
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
    private final Vertex outVertex_, inVertex_;
    private final String label_;

    private static long edgeID_ = 0;
}
