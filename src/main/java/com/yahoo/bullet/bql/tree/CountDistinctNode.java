/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class CountDistinctNode extends ExpressionNode {
    private final List<ExpressionNode> expressions;

    public CountDistinctNode(List<ExpressionNode> expressions, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expressions = expressions;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitCountDistinct(this, context);
    }

    @Override
    public List<ExpressionNode> getChildren() {
        return expressions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof CountDistinctNode && Objects.equals(expressions, ((CountDistinctNode) obj).expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions);
    }
}
