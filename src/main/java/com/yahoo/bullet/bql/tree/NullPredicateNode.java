/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

@Getter
public class NullPredicateNode extends ExpressionNode {
    private final ExpressionNode expression;
    private final boolean not;

    public NullPredicateNode(ExpressionNode expression, boolean not, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expression = expression;
        this.not = not;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNullPredicate(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NullPredicateNode)) {
            return false;
        }
        NullPredicateNode other = (NullPredicateNode) obj;
        return Objects.equals(expression, other.expression) && not == other.not;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, not);
    }
}
