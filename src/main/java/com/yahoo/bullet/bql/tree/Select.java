/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Select.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Select extends Node {
    private final boolean distinct;
    private final List<SelectItem> selectItems;

    /**
     * Constructor that requires a boolean distinct, and a List of {@link SelectItem}.
     *
     * @param distinct    A boolean.
     * @param selectItems A List of {@link SelectItem}.
     */
    public Select(boolean distinct, List<SelectItem> selectItems) {
        this(Optional.empty(), distinct, selectItems);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a boolean distinct, and a List of {@link SelectItem}.
     *
     * @param location    A {@link NodeLocation}.
     * @param distinct    A boolean.
     * @param selectItems A List of {@link SelectItem}.
     */
    public Select(NodeLocation location, boolean distinct, List<SelectItem> selectItems) {
        this(Optional.of(location), distinct, selectItems);
    }

    private Select(Optional<NodeLocation> location, boolean distinct, List<SelectItem> selectItems) {
        super(location);
        this.distinct = distinct;
        this.selectItems = ImmutableList.copyOf(requireNonNull(selectItems, "selectItems"));
    }

    /**
     * Get the {@link #distinct} of this Select.
     *
     * @return A boolean.
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Get the {@link #selectItems} of this Select.
     *
     * @return A List of {@link SelectItem}.
     */
    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return selectItems;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("distinct", distinct)
                .add("selectItems", selectItems)
                .omitNullValues()
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

        Select select = (Select) o;
        return (distinct == select.distinct) &&
                Objects.equals(selectItems, select.selectItems);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinct, selectItems);
    }
}
