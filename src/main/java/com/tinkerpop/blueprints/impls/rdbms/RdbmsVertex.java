// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newHashSet;

import java.util.*;


import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.VerticesFromEdgesIterable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RdbmsVertex extends RdbmsElement implements Vertex {

    // =================================
    public RdbmsVertex(final long vertexID, final RdbmsGraph graph) {
        super(graph.vertexPropertyCache(), vertexID, graph);
        log.info("RdbmsVertex ctor");
    // TODO: pull ids, instead of edges?
        for (RdbmsEdge e: graph.getEdges(vertexID)) {
            if (vertexID == e.getOutVertexId())
                qOutEdges.add(e.rawId());
            if (vertexID == e.getInVertexId())
                qInEdges.add(e.rawId());
        }
    }
    // =================================
    // Not so efficient, looking up one at a time...?
    private Function<Long, Edge> lookupEdge = new Function<Long, Edge>() {
        @Override
        public Edge apply(Long id) {
            return getGraph().getEdge(id);
        }
    };
    // =================================
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        log.info("getEdges for {} {}", this, direction);
        if (direction.equals(Direction.OUT)) {
            return getEdges(qOutEdges, Arrays.asList(labels));
        } else if (direction.equals(Direction.IN)) {
            return getEdges(qInEdges, Arrays.asList(labels));
        } else {
            return concat(getEdges(qInEdges, Arrays.asList(labels)),
                          getEdges(qOutEdges, Arrays.asList(labels)));
        }
    }
    // =================================
    // annoyingly, an empty label list is a special case for 'all'
    private Iterable<Edge> getEdges(Iterable<Long> edgeIDs, final Collection<String> labels) {
        Iterable<Edge> i = transform(edgeIDs, lookupEdge);
        if (labels.isEmpty())
            return i;
        return filter(i, new Predicate<Edge>() {
            @Override
            public boolean apply(Edge e) {
                return labels.contains(e.getLabel());
            }
        });
    }
    // =================================
    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        // TODO: ???
        return new VerticesFromEdgesIterable(this, direction, labels);
    }
    // =================================
    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }
    // =================================
    @Override
    public Edge addEdge(String label, Vertex vertex) {
        return getGraph().addEdge(null, this, vertex, label);
    }
    // =================================
    void addOutEdge(Long edgeId) {
        qOutEdges.add(edgeId);
    }
    // =================================
    void addInEdge(Long edgeId) {
        qInEdges.add(edgeId);
    }
    // =================================
    @Override
    public void remove() {
        getGraph().removeVertex(this);
    }
    // =================================
    void removeEdge(Long edgeId) {
        qOutEdges.remove(edgeId);
        qInEdges.remove(edgeId);
    }

    // =================================
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    // =================================
    // yay, type-erasure.
    private static final Set<Long> typedSet() { return newHashSet(); };
    private Set<Long> qOutEdges = Collections.synchronizedSet(typedSet());
    private Set<Long> qInEdges  = Collections.synchronizedSet(typedSet());


}
