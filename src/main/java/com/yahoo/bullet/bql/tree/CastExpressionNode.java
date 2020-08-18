/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

@Getter
public class CastExpressionNode extends ExpressionNode {
    private final ExpressionNode expression;
    private final Type castType;

    public CastExpressionNode(ExpressionNode expression, Type castType, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.expression = expression;
        this.castType = castType;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitCastExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CastExpressionNode)) {
            return false;
        }
        CastExpressionNode other = (CastExpressionNode) obj;
        return Objects.equals(expression, other.expression) && castType == other.castType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, castType);
    }
}
