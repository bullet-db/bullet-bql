/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Assumed to be EXPLODE wherever it's used currently.
 */
@Getter
public class TableFunctionNode extends ExpressionNode {
    private final TableFunctionType type;
    private final ExpressionNode expression;
    private final IdentifierNode keyAlias;
    private final IdentifierNode valueAlias;
    private final boolean outer;

    public TableFunctionNode(TableFunctionType type, ExpressionNode expression, IdentifierNode keyAlias,
                             IdentifierNode valueAlias, boolean outer, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.type = type;
        this.expression = expression;
        this.keyAlias = keyAlias;
        this.valueAlias = valueAlias;
        this.outer = outer;
    }

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitTableFunction(this, context);
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
        if (!(obj instanceof TableFunctionNode)) {
            return false;
        }
        TableFunctionNode other = (TableFunctionNode) obj;
        return type == other.type &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(keyAlias, other.keyAlias) &&
               Objects.equals(valueAlias, other.valueAlias) &&
               outer == other.outer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, expression, keyAlias, valueAlias, outer);
    }
}
