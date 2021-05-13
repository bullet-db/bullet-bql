/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

@Getter
public class LateralViewNode extends Node {
    private final TableFunctionNode tableFunction;

    public LateralViewNode(TableFunctionNode tableFunction, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.tableFunction = tableFunction;
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
        return Objects.equals(tableFunction, other.tableFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableFunction);
    }
}
