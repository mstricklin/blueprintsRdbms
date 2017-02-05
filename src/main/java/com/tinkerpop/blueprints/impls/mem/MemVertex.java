// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
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
public class MemVertex extends MemElement implements Vertex {

    public MemVertex(final MemGraph graph) {
        super(vertexID_++, graph);
    }
    // =================================
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            return getEdges(outEdges_, Arrays.asList(labels));
        } else if (direction.equals(Direction.IN)) {
            return getEdges(inEdges_, Arrays.asList(labels));
        } else {
            return concat(getEdges(inEdges_,  Arrays.asList(labels)),
                          getEdges(outEdges_, Arrays.asList(labels)));
        }
    }
    // =================================
    // annoyingly, an empty list is a special case for 'all'
    private Iterable<Edge> getEdges(Multimap<String, Edge> edges, Collection<String> labels) {
        if (labels.isEmpty())
            return newArrayList(edges.values());
        Multimap<String, Edge> m = Multimaps.filterKeys(edges, Predicates.in(labels));
        return newArrayList(m.values());
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
        return getGraph().addEdge(null, this, vertex, label);
    }
    // =================================
    public void addOutEdge(String label, Edge e) {
        outEdges_.put(label,  e);
    }
    // =================================
    public void addInEdge(String label, Edge e) {
        inEdges_.put(label,  e);
    }
    // =================================
    @Override
    public void remove() {
        getGraph().removeVertex(this);
    }
    // =================================
    public void removeEdge(Edge edge) {
        // is it in, or out? easier to move from both...
        outEdges_.remove(edge.getLabel(), edge);
        inEdges_.remove(edge.getLabel(), edge);
    }
    // =================================
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    // =================================
    private static long vertexID_ = 0;
    private Multimap<String, Edge> outEdges_ = ArrayListMultimap.create();
    private Multimap<String, Edge> inEdges_  = ArrayListMultimap.create();

}
