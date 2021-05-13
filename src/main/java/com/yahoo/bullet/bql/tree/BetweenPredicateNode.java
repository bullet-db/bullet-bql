/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class BetweenPredicateNode extends ExpressionNode {
    private final ExpressionNode expression;
    private final ExpressionNode lower;
    private final ExpressionNode upper;
    private final boolean not;

    public BetweenPredicateNode(ExpressionNode expression, ExpressionNode lower, ExpressionNode upper, boolean not, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expression = expression;
        this.lower = lower;
        this.upper = upper;
        this.not = not;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public List<ExpressionNode> getChildren() {
        return Collections.singletonList(expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BetweenPredicateNode)) {
            return false;
        }
        BetweenPredicateNode other = (BetweenPredicateNode) obj;
        return Objects.equals(expression, other.expression) &&
               Objects.equals(lower, other.lower) &&
               Objects.equals(upper, other.upper) &&
               not == other.not;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, lower, upper, not);
    }
}
