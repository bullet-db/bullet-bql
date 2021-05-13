/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
public class FieldExpressionNode extends ExpressionNode {
    protected final IdentifierNode field;
    // Types ignored for equals() and hashCode()
    @Setter
    private Type type;

    public FieldExpressionNode(IdentifierNode field, Type type, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.field = field;
        this.type = type;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitFieldExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FieldExpressionNode)) {
            return false;
        }
        FieldExpressionNode other = (FieldExpressionNode) obj;
        return Objects.equals(field, other.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
