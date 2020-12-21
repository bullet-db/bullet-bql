/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.expressions.Operation;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class UnaryExpressionNode extends ExpressionNode {
    private final Operation op;
    private final ExpressionNode expression;
    // Not used for equals() and hashCode()
    private final boolean parenthesized;

    public UnaryExpressionNode(Operation op, ExpressionNode expression, boolean parenthesized, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.op = op;
        this.expression = expression;
        this.parenthesized = parenthesized;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitUnaryExpression(this, context);
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
        if (!(obj instanceof UnaryExpressionNode)) {
            return false;
        }
        UnaryExpressionNode other = (UnaryExpressionNode) obj;
        return op == other.op && Objects.equals(expression, other.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expression);
    }
}
