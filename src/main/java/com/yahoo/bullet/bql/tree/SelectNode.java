/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Select.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class SelectNode extends Node {
    private final boolean distinct;
    private final List<SelectItemNode> selectItems;

    public SelectNode(boolean distinct, List<SelectItemNode> selectItems, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.distinct = distinct;
        this.selectItems = selectItems;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SelectNode)) {
            return false;
        }
        SelectNode other = (SelectNode) obj;
        return distinct == other.distinct && Objects.equals(selectItems, other.selectItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinct, selectItems);
    }
}
