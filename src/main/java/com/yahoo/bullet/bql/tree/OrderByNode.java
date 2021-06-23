/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/OrderBy.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class OrderByNode extends Node {
    private final List<SortItemNode> sortItems;

    public OrderByNode(List<SortItemNode> sortItems, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.sortItems = sortItems;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitOrderBy(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OrderByNode)) {
            return false;
        }
        OrderByNode other = (OrderByNode) obj;
        return Objects.equals(sortItems, other.sortItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortItems);
    }
}
