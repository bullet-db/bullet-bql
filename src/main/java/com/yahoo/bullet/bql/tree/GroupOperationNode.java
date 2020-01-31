/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;

@Getter
@RequiredArgsConstructor
public class GroupOperationNode extends ExpressionNode {
    private final GroupOperationType op;
    private final ExpressionNode expression;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitGroupOperation(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof GroupOperationNode)) {
            return false;
        }
        GroupOperationNode other = (GroupOperationNode) obj;
        return op == other.op && Objects.equals(expression, other.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expression);
    }
}
