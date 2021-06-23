/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.expressions.Operation;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Getter
public class BinaryExpressionNode extends ExpressionNode {
    private final ExpressionNode left;
    private final ExpressionNode right;
    private final Operation op;

    public BinaryExpressionNode(ExpressionNode left, ExpressionNode right, Operation op, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryExpression(this, context);
    }

    @Override
    public List<ExpressionNode> getChildren() {
        return Arrays.asList(left, right);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BinaryExpressionNode)) {
            return false;
        }
        BinaryExpressionNode other = (BinaryExpressionNode) obj;
        return Objects.equals(left, other.left) &&
               Objects.equals(right, other.right) &&
               op == other.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op);
    }
}
