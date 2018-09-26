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
    private final List<SortItem> sortItems;

    /**
     * Constructor that requires a List of {@link SortItem}.
     *
     * @param sortItems A List of {@link SortItem}.
     */
    public OrderBy(List<SortItem> sortItems) {
        this(Optional.empty(), sortItems);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a List of {@link SortItem}.
     *
     * @param location  A {@link NodeLocation}.
     * @param sortItems A List of {@link SortItem}.
     */
    public OrderBy(NodeLocation location, List<SortItem> sortItems) {
        this(Optional.of(location), sortItems);
    }

    private OrderBy(Optional<NodeLocation> location, List<SortItem> sortItems) {
        super(location);
        requireNonNull(sortItems, "sortItems is null");
        checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
        this.sortItems = ImmutableList.copyOf(sortItems);
    }

    /**
     * Get the {@link #sortItems} of this OrderBy.
     *
     * @return A List of {@link SortItem}.
     */
    public List<SortItem> getSortItems() {
        return sortItems;
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
        return Objects.equals(sortItems, o.sortItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortItems);
    }
}
