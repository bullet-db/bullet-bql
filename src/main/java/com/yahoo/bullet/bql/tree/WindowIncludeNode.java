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

@Getter
@RequiredArgsConstructor
public class WindowIncludeNode extends Node {
    private final Long first;
    private final Unit includeUnit;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindowInclude(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WindowIncludeNode)) {
            return false;
        }
        WindowIncludeNode other = (WindowIncludeNode) obj;
        return Objects.equals(first, other.first) && includeUnit == other.includeUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, includeUnit);
    }
}
