/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

@Getter
public class FieldExpressionNode extends ExpressionNode {
    private final IdentifierNode field;
    private final Integer index;
    private final IdentifierNode key;
    private final IdentifierNode subKey;
    // Types ignored for equals() and hashCode()
    private final Type type;

    public FieldExpressionNode(IdentifierNode field, Integer index, IdentifierNode key, IdentifierNode subKey, Type type, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.field = field;
        this.index = index;
        this.key = key;
        this.subKey = subKey;
        this.type = type;
    }

    public boolean hasIndexOrKey() {
        return index != null || key != null;
    }

    public boolean hasSubKey() {
        return subKey != null;
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
        return Objects.equals(field, other.field) &&
               Objects.equals(index, other.index) &&
               Objects.equals(key, other.key) &&
               Objects.equals(subKey, other.subKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, subKey);
    }
}
