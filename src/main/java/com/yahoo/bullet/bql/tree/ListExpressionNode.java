/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class ListExpressionNode extends ExpressionNode {
    private final List<ExpressionNode> expressions;
    // Not used for equals() and hashCode()
    private final boolean parenthesized;

    public ListExpressionNode(List<ExpressionNode> expressions, boolean parenthesized, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expressions = expressions;
        this.parenthesized = parenthesized;
    }

    public ListExpressionNode(List<ExpressionNode> expressions, NodeLocation nodeLocation) {
        this(expressions, false, nodeLocation);
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitListExpression(this, context);
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
        return obj instanceof ListExpressionNode && Objects.equals(expressions, ((ListExpressionNode) obj).expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions);
    }
}
