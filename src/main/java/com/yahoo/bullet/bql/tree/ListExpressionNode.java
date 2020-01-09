/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class ListExpressionNode extends ExpressionNode {
    private final List<ExpressionNode> expressions;

    // TODO restrict to primitives somehow............?

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitListExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ListExpressionNode && Objects.equals(expressions, ((ListExpressionNode) obj).expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions);
    }
}
