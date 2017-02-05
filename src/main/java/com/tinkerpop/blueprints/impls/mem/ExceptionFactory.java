// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.mem;

public class ExceptionFactory {
    public static IllegalArgumentException vertexIdCanNotBeNull() {
        return new IllegalArgumentException("Vertex id can not be null");
    }

    public static IllegalArgumentException edgeIdCanNotBeNull() {
        return new IllegalArgumentException("Edge id can not be null");
    }

    public static IllegalArgumentException bothIsNotSupported() {
        return new IllegalArgumentException("A direction of BOTH is not supported");
    }
}
