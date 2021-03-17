/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class SubFieldExpressionNode extends ExpressionNode {
    private final ExpressionNode field;
    private final Integer index;
    private final IdentifierNode key;
    private final ExpressionNode expressionKey;
    private final String stringKey;
    // Types ignored for equals() and hashCode()
    @Setter
    private Type type;

    public SubFieldExpressionNode(ExpressionNode field, Integer index, IdentifierNode key, ExpressionNode expressionKey,
                                  String stringKey, Type type, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.field = field;
        this.index = index;
        this.key = key;
        this.expressionKey = expressionKey;
        this.stringKey = stringKey;
        this.type = type;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSubFieldExpression(this, context);
    }

    @Override
    public List<ExpressionNode> getChildren() {
        if (expressionKey != null) {
            return Arrays.asList(field, expressionKey);
        }
        return Collections.singletonList(field);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SubFieldExpressionNode)) {
            return false;
        }
        SubFieldExpressionNode other = (SubFieldExpressionNode) obj;
        return Objects.equals(field, other.field) &&
               Objects.equals(index, other.index) &&
               Objects.equals(key, other.key) &&
               Objects.equals(expressionKey, other.expressionKey) &&
               Objects.equals(stringKey, other.stringKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, expressionKey, stringKey);
    }
}
