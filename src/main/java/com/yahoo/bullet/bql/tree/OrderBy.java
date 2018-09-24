/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/OrderBy.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrderBy extends Node {
    public enum Ordering {
        ASCENDING, DESCENDING
    }

    private final List<SortItem> sortItems;
    private final Ordering ordering;

    /**
     * Constructor that requires a List of {@link SortItem}.
     *
     * @param sortItems A List of {@link SortItem}.
     */
    public OrderBy(List<SortItem> sortItems) {
        this(Optional.empty(), sortItems, Ordering.ASCENDING);
    }

    /**
     * Constructor that requires a List of {@link SortItem} and an {@link Ordering}.
     *
     * @param sortItems A List of {@link SortItem}.
     * @param ordering An {@link Ordering}.
     */
    public OrderBy(List<SortItem> sortItems, Ordering ordering) {
        this(Optional.empty(), sortItems, ordering);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link SortItem}, and an {@link Ordering}.
     *
     * @param location  A {@link NodeLocation}.
     * @param sortItems A List of {@link SortItem}.
     * @param ordering An {@link Ordering}.
     */
    public OrderBy(NodeLocation location, List<SortItem> sortItems, Ordering ordering) {
        this(Optional.of(location), sortItems, ordering);
    }

    private OrderBy(Optional<NodeLocation> location, List<SortItem> sortItems, Ordering ordering) {
        super(location);
        requireNonNull(sortItems, "sortItems is null");
        requireNonNull(ordering, "ordering is null");
        checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
        this.sortItems = ImmutableList.copyOf(sortItems);
        this.ordering = ordering;
    }

    /**
     * Get the {@link #sortItems} of this OrderBy.
     *
     * @return A List of {@link SortItem}.
     */
    public List<SortItem> getSortItems() {
        return sortItems;
    }

    /**
     * Get the {@link Ordering} of this OrderBy.
     *
     * @return An {@link Ordering}.
     */
    public Ordering getOrdering() {
        return ordering;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitOrderBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return sortItems;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sortItems", sortItems)
                .add("ordering", ordering)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OrderBy o = (OrderBy) obj;
        return Objects.equals(sortItems, o.sortItems) && (ordering == o.ordering);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortItems, ordering);
    }
}
