// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import static com.google.common.collect.Maps.newHashMap;

import java.util.*;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.util.ElementHelper;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class RdbmsElement implements Element {

    RdbmsElement(final long id_, final RdbmsGraph graph_) {
        id = id_;
        graph = graph_;
        dao = graph.getDaoFactory().getPropertyDao();
    }
    // =================================
    @Override
    public abstract void remove();
    // =================================
    @Override
    public Object getId() {
        return this.id;
    }
    protected long rawId() {
        return this.id;
    }
    // =================================
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    // =================================
    protected RdbmsGraph getGraph() {
        return graph;
    }
    // =================================
    @Override
    public int hashCode() {
        return (int)id;
    }
    // =================================
    protected final RdbmsGraph graph;
    protected final PropertyDao dao;
    protected final long id;
    // =================================
    @RequiredArgsConstructor(staticName = "of")
    @Data
    public static final class PropertyDTO {
        public final String key;
        public final Object value;
        public static Map<String, Object> toMap(Collection<PropertyDTO> c) {
            Map<String, Object> m = newHashMap();
            for (PropertyDTO p: c)
                m.put(p.key, p.value);
            return m;
        }
    }

}
