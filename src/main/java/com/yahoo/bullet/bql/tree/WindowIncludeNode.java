/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window.Unit;
import lombok.Getter;

@Getter
public class WindowIncludeNode extends Node {
    private final Long first;
    private final Unit includeUnit;

    /**
     * Constructs a WindowIncludeNode from a {@link String} first and a {@link String} include unit.
     *
     * @param first The first as a {@link String}.
     * @param includeUnit The include unit as a {@link String}.
     */
    public WindowIncludeNode(String first, String includeUnit) {
        this.first = first != null ? Long.parseLong(first) : null;
        this.includeUnit = Unit.valueOf(includeUnit.toUpperCase());
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindowInclude(this, context);
    }
}
