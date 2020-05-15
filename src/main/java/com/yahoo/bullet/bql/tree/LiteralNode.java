/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class LiteralNode extends ExpressionNode {
    private final Serializable value;

    public LiteralNode(Serializable value, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.value = value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof LiteralNode && Objects.equals(value, ((LiteralNode) obj).value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
