/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Getter
@RequiredArgsConstructor
public class StreamNode extends Node {
    private final String timeDuration;
    private final String recordDuration;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitStream(this, context);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("timeDuration", timeDuration)
                                   .add("recordDuration", recordDuration)
                                   .toString();
    }
}
