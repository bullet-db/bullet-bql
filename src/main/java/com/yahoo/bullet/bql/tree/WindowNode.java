/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.Window.Unit;
import lombok.Getter;

import java.util.Objects;

@Getter
public class WindowNode extends Node {
    private final Integer emitEvery;
    private final Unit emitType;
    private final WindowIncludeNode windowInclude;

    public WindowNode(Integer emitEvery, Unit emitType, WindowIncludeNode windowInclude, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.emitEvery = emitEvery;
        this.emitType = emitType;
        this.windowInclude = windowInclude;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindow(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WindowNode)) {
            return false;
        }
        WindowNode other = (WindowNode) obj;
        return Objects.equals(emitEvery, other.emitEvery) &&
               emitType == other.emitType &&
               Objects.equals(windowInclude, other.windowInclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emitEvery, emitType, windowInclude);
    }
}
