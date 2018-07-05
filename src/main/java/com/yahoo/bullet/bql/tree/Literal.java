/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Literal.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public abstract class Literal extends Expression {
    /**
     * Constructor that requires an Optional of {@link Expression}.
     *
     * @param location An Optional of {@link Expression}.
     */
    protected Literal(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }
}
