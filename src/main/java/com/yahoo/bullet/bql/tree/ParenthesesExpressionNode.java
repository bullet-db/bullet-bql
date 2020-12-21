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
public class ParenthesesExpressionNode extends ExpressionNode {
    private final ExpressionNode expression;

    public ParenthesesExpressionNode(ExpressionNode expression, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expression = expression;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitParenthesesExpression(this, context);
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
        return obj instanceof ParenthesesExpressionNode && Objects.equals(expression, ((ParenthesesExpressionNode) obj).expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression);
    }
}
