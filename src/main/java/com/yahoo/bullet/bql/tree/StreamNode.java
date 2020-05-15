/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

@Getter
public class StreamNode extends Node {
    private final String timeDuration;

    public StreamNode(String timeDuration, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.timeDuration = timeDuration;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitStream(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof StreamNode)) {
            return false;
        }
        StreamNode other = (StreamNode) obj;
        return Objects.equals(timeDuration, other.timeDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeDuration);
    }
}
