/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SelectItem.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class SelectItemNode extends Node {
    private final boolean all;
    private final ExpressionNode expression;
    private final IdentifierNode alias;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSelectItem(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SelectItemNode)) {
            return false;
        }
        SelectItemNode other = (SelectItemNode) obj;
        return all == other.all &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(alias, other.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(all, expression, alias);
    }

    @Override
    public String toString() {
        return null;
    }
}
