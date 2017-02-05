// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import lombok.extern.slf4j.Slf4j;


//SELECT vertex.id, vertex_id, key, value FROM VERTEX
//left outer join vertex_property on vertex.id = vertex_property.vertex_id
//where id = 0


@Slf4j
public class VertexDao {

    public RdbmsVertex getVertex(Object id) {
        return null;
    }
}
