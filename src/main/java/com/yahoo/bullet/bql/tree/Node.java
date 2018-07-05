/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Node.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Node {
    private final Optional<NodeLocation> location;

    /**
     * Constructor that requires an Optional of {@link NodeLocation}.
     *
     * @param location An Optional of {@link NodeLocation}.
     */
    protected Node(Optional<NodeLocation> location) {
        this.location = requireNonNull(location, "location is null");
    }

    /*
     * accessible for {@link ASTVisitor}, use {@link ASTVisitor#process(Node, Object)} instead.
     */

    /**
     * Make Node accessible for {@link ASTVisitor}, use {@link ASTVisitor#process(Node, Object)}.
     *
     * @param visitor An implementation of {@link ASTVisitor}.
     * @param context A {@link C} that passed in.
     * @param <R>     The return type of {@link ASTVisitor#process(Node, Object)}.
     * @param <C>     The context type of {@link ASTVisitor#process(Node, Object)}.
     * @return A {@link R}.
     */
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }

    /**
     * Get an Optional of the {@link NodeLocation} of this Node.
     *
     * @return An Optional of the {@link NodeLocation}.
     */
    public Optional<NodeLocation> getLocation() {
        return location;
    }

    /**
     * Get the children of this Node.
     *
     * @return A List of subclasses of {@link Node}.
     */
    public abstract List<? extends Node> getChildren();

    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
