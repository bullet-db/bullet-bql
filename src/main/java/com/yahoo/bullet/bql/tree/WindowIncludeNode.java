/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window.Unit;
import lombok.Getter;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Getter
public class WindowIncludeNode extends Node {
    private final Long number;
    private final Unit unit;

    public WindowIncludeNode(String number, String unit) {
        this.number = number != null ? Long.parseLong(number) : null;
        this.unit = Unit.valueOf(unit.toUpperCase());
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindowInclude(this, context);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("number", number)
                                   .add("unit", unit)
                                   .omitNullValues()
                                   .toString();
    }
}
