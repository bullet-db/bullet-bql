/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class LateralViewNode extends Node {
    private final List<TableFunctionNode> tableFunctions;

    public LateralViewNode(TableFunctionNode tableFunction, NodeLocation nodeLocation) {
        this(Collections.singletonList(tableFunction), nodeLocation);
    }

    public LateralViewNode(List<TableFunctionNode> tableFunctions, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.tableFunctions = tableFunctions;
    }

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLateralView(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LateralViewNode)) {
            return false;
        }
        LateralViewNode other = (LateralViewNode) obj;
        return Objects.equals(tableFunctions, other.tableFunctions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableFunctions);
    }
}
