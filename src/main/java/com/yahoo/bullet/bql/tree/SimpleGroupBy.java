/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SimpleGroupBy.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SimpleGroupBy extends GroupingElement {
    private final List<Expression> columns;

    /**
     * Constructor that requires a List of {@link Expression}.
     *
     * @param simpleGroupByExpressions A List of {@link Expression}.
     */
    public SimpleGroupBy(List<Expression> simpleGroupByExpressions) {
        this(Optional.empty(), simpleGroupByExpressions);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a List of {@link Expression}.
     *
     * @param location                 A {@link NodeLocation}.
     * @param simpleGroupByExpressions A List of {@link Expression}.
     */
    public SimpleGroupBy(NodeLocation location, List<Expression> simpleGroupByExpressions) {
        this(Optional.of(location), simpleGroupByExpressions);
    }

    private SimpleGroupBy(Optional<NodeLocation> location, List<Expression> simpleGroupByExpressions) {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(simpleGroupByExpressions, "simpleGroupByExpressions is null"));
    }

    /**
     * Get the {@link #columns} of this SimpleGroupBy.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getColumnExpressions() {
        return columns;
    }

    @Override
    public List<Set<Expression>> enumerateGroupingSets() {
        return ImmutableList.of(ImmutableSet.copyOf(columns));
    }

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSimpleGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleGroupBy that = (SimpleGroupBy) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }
}
