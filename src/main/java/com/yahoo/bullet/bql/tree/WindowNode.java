/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window.Unit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Getter
@RequiredArgsConstructor
public class WindowNode extends Node {
    private final Long emitEvery;
    private final Unit emitType;
    private final WindowIncludeNode windowInclude;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindow(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WindowNode)) {
            return false;
        }
        WindowNode other = (WindowNode) obj;
        return Objects.equals(emitEvery, other.emitEvery) &&
               Objects.equals(emitType, other.emitType) &&
               Objects.equals(windowInclude, other.windowInclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emitEvery, emitType, windowInclude);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("emitEvery", emitEvery)
                                   .add("emitType", emitType)
                                   .add("windowInclude", windowInclude)
                                   .toString();
    }
}
