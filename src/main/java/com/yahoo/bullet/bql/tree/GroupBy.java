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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GroupBy extends Node {
    private final boolean isDistinct;
    private final List<GroupingElement> groupingElements;

    /**
     * Constructor that requires a boolean isDistinct and a List of {@link GroupingElement}.
     *
     * @param isDistinct       A boolean.
     * @param groupingElements A List of {@link GroupingElement}.
     */
    public GroupBy(boolean isDistinct, List<GroupingElement> groupingElements) {
        this(Optional.empty(), isDistinct, groupingElements);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a boolean isDistinct and a List of {@link GroupingElement}.
     *
     * @param location         A {@link NodeLocation}.
     * @param isDistinct       A boolean.
     * @param groupingElements A List of {@link GroupingElement}.
     */
    public GroupBy(NodeLocation location, boolean isDistinct, List<GroupingElement> groupingElements) {
        this(Optional.of(location), isDistinct, groupingElements);
    }

    private GroupBy(Optional<NodeLocation> location, boolean isDistinct, List<GroupingElement> groupingElements) {
        super(location);
        this.isDistinct = isDistinct;
        this.groupingElements = ImmutableList.copyOf(requireNonNull(groupingElements));
    }

    /**
     * Get the {@link #isDistinct} of this GroupBy.
     *
     * @return A boolean.
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * Get the {@link #groupingElements} of this GroupBy.
     *
     * @return A List of {@link GroupingElement}.
     */
    public List<GroupingElement> getGroupingElements() {
        return groupingElements;
    }

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return groupingElements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupBy groupBy = (GroupBy) o;
        return isDistinct == groupBy.isDistinct &&
                Objects.equals(groupingElements, groupBy.groupingElements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, groupingElements);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("isDistinct", isDistinct)
                .add("groupingElements", groupingElements)
                .toString();
    }
}
