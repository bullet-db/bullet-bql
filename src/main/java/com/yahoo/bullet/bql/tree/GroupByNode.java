/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/GroupBy.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Getter
@RequiredArgsConstructor
public class GroupByNode extends Node {
    private final List<ExpressionNode> expressions;

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitGroupBy(this, context);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("expressions", expressions).toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GroupByNode && Objects.equals(expressions, ((GroupByNode) obj).expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions);
    }
}
