/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.expressions.Operation;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class NAryExpressionNode extends ExpressionNode {
    private final Operation op;
    private final List<ExpressionNode> expressions;

    public NAryExpressionNode(Operation op, List<ExpressionNode> expressions, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.op = op;
        this.expressions = expressions;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNAryExpression(this, context);
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
        if (!(obj instanceof NAryExpressionNode)) {
            return false;
        }
        NAryExpressionNode other = (NAryExpressionNode) obj;
        return op == other.op && Objects.equals(expressions, other.expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expressions);
    }
}
