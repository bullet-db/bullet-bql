/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/InListExpression.java
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
