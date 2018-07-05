/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SortItem.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SortItem extends Node {
    public enum Ordering {
        ASCENDING, DESCENDING
    }

    public enum NullOrdering {
        FIRST, LAST, UNDEFINED
    }

    private final Expression sortKey;
    private final Ordering ordering;
    private final NullOrdering nullOrdering;

    /**
     * Constructor that requires an {@link Expression} sortKey, an {@link Ordering} and a {@link NullOrdering}.
     *
     * @param sortKey      An {@link Expression}.
     * @param ordering     An {@link Ordering}.
     * @param nullOrdering A {@link NullOrdering}.
     */
    public SortItem(Expression sortKey, Ordering ordering, NullOrdering nullOrdering) {
        this(Optional.empty(), sortKey, ordering, nullOrdering);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} sortKey, an {@link Ordering} and a {@link NullOrdering}.
     *
     * @param location     A {@link NodeLocation}.
     * @param sortKey      An {@link Expression}.
     * @param ordering     An {@link Ordering}.
     * @param nullOrdering A {@link NullOrdering}.
     */
    public SortItem(NodeLocation location, Expression sortKey, Ordering ordering, NullOrdering nullOrdering) {
        this(Optional.of(location), sortKey, ordering, nullOrdering);
    }

    private SortItem(Optional<NodeLocation> location, Expression sortKey, Ordering ordering, NullOrdering nullOrdering) {
        super(location);
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    /**
     * Get the {@link #sortKey} of this SortItem.
     *
     * @return An {@link Expression}.
     */
    public Expression getSortKey() {
        return sortKey;
    }

    /**
     * Get the {@link #ordering} of this SortItem.
     *
     * @return An {@link Ordering}.
     */
    public Ordering getOrdering() {
        return ordering;
    }

    /**
     * Get the {@link #nullOrdering} of this SortItem.
     *
     * @return A {@link NullOrdering}.
     */
    public NullOrdering getNullOrdering() {
        return nullOrdering;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSortItem(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(sortKey);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sortKey", sortKey)
                .add("ordering", ordering)
                .add("nullOrdering", nullOrdering)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SortItem sortItem = (SortItem) o;
        return Objects.equals(sortKey, sortItem.sortKey) &&
                (ordering == sortItem.ordering) &&
                (nullOrdering == sortItem.nullOrdering);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortKey, ordering, nullOrdering);
    }
}
