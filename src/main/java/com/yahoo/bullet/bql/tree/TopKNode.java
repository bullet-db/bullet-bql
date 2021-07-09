/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class TopKNode extends ExpressionNode {
    private final Integer size;
    private final Long threshold;
    private final List<ExpressionNode> expressions;

    public TopKNode(Integer size, Long threshold, List<ExpressionNode> expressions, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.size = size;
        this.threshold = threshold;
        this.expressions = expressions;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitTopK(this, context);
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
        if (!(obj instanceof TopKNode)) {
            return false;
        }
        TopKNode other = (TopKNode) obj;
        return Objects.equals(size, other.size) &&
               Objects.equals(threshold, other.threshold) &&
               Objects.equals(expressions, other.expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, threshold, expressions);
    }
}
