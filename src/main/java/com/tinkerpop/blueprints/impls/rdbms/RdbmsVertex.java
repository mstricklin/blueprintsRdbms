// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Iterables.concat;

import java.util.Arrays;
import java.util.Collection;


import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.VerticesFromEdgesIterable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RdbmsVertex extends RdbmsElement implements Vertex {

    // =================================
    public RdbmsVertex(final int vertexID, final RdbmsGraph graph) {
        super(vertexID, graph);
        // TODO: populate edges
    }

    // =================================
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        log.info("getEdges for {} {}", this, direction);
        if (direction.equals(Direction.OUT)) {
            return getEdges(outEdges, Arrays.asList(labels));
        } else if (direction.equals(Direction.IN)) {
            return getEdges(inEdges, Arrays.asList(labels));
        } else {
            return concat(getEdges(inEdges, Arrays.asList(labels)), getEdges(outEdges, Arrays.asList(labels)));
        }
    }
    // =================================
    // annoyingly, an empty label list is a special case for 'all'
    private Iterable<Edge> getEdges(Multimap<String, Edge> edges, Collection<String> labels) {
        if (labels.isEmpty())
            return ImmutableList.copyOf(edges.values());
        Multimap<String, Edge> m = Multimaps.filterKeys(edges, Predicates.in(labels));
        return ImmutableList.copyOf(m.values());
    }

    // =================================
    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
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
        // add to self list
        return getGraph().addEdge(null, this, vertex, label);
    }

    // =================================
    protected void addOutEdge(String label, Edge e) {
        outEdges.put(label, e);
    }

    // =================================
    protected void addInEdge(String label, Edge e) {
        inEdges.put(label, e);
    }
    // =================================
    @Override
    public void remove() {
        getGraph().removeVertex(this);
    }
    // =================================
    public void removeEdge(Edge edge) {
        // is it in, or out? easier to remove from both...
        outEdges.remove(edge.getLabel(), edge);
        inEdges.remove(edge.getLabel(), edge);
    }
    // =================================
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    // =================================
    // yay, type-erasure.
    private static final Multimap<String, Edge> typedMultiMap() {
        return ArrayListMultimap.create();
    }
    private Multimap<String, Edge> outEdges = Multimaps.synchronizedMultimap(typedMultiMap());
    private Multimap<String, Edge> inEdges = Multimaps.synchronizedMultimap(typedMultiMap());

}
