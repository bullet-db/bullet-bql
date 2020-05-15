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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class Node {
    @Getter
    private final NodeLocation location;

    /**
     * Makes Node accessible for {@link ASTVisitor}; use {@link ASTVisitor#process(Node, Object)}.
     *
     * @param visitor An implementation of {@link ASTVisitor}.
     * @param context A {@link C} that passed in.
     * @param <R> The return type of {@link ASTVisitor#process(Node, Object)}.
     * @param <C> The context type of {@link ASTVisitor#process(Node, Object)}.
     * @return A {@link R}.
     */
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();
}
