/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SortItem.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static com.google.common.base.MoreObjects.toStringHelper;

@Getter
@RequiredArgsConstructor
public class SortItemNode extends Node {
    public enum Ordering {
        ASCENDING,
        DESCENDING
    }

    private final ExpressionNode expression;
    private final Ordering ordering;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSortItem(this, context);
    }
/*
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SortItemNode)) {
            return false;
        }
        SortItemNode other = (SortItemNode) obj;
        return Objects.equals(expression, other.expression) && ordering == other.ordering;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, ordering);
    }
*/
    @Override
    public String toString() {
        return toStringHelper(this).add("expression", expression)
                                   .add("ordering", ordering)
                                   .toString();
    }
}
