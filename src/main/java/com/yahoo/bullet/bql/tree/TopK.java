/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TopK extends Expression {
    private final List<Expression> columns;
    private final Long size;
    private final Optional<Long> threshold;

    /**
     * Constructor that requires a List of {@link Expression} columns, a Long size and an Optional of Long threshold.
     *
     * @param columns   A List of {@link Expression}.
     * @param size      A Long.
     * @param threshold An Optional of Long.
     */
    public TopK(List<Expression> columns, Long size, Optional<Long> threshold) {
        this(Optional.empty(), columns, size, threshold);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link Expression} columns, a Long size and an Optional of Long threshold.
     *
     * @param location  A {@link NodeLocation}.
     * @param columns   A List of {@link Expression}.
     * @param size      A Long.
     * @param threshold An Optional of Long.
     */
    public TopK(NodeLocation location, List<Expression> columns, Long size, Optional<Long> threshold) {
        this(Optional.of(location), columns, size, threshold);
    }

    private TopK(Optional<NodeLocation> location, List<Expression> columns, Long size, Optional<Long> threshold) {
        super(location);
        this.columns = requireNonNull(columns, "selectItems is null");
        this.size = requireNonNull(size, "size is null");
        this.threshold = requireNonNull(threshold, "threshold is null");
    }

    /**
     * Get the {@link #columns} of this TopK.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getColumns() {
        return columns;
    }

    /**
     * Get the {@link #size} of this TopK.
     *
     * @return A Long.
     */
    public Long getSize() {
        return size;
    }

    /**
     * Get the {@link #threshold} of this TopK.
     *
     * @return An Optional of Long.
     */
    public Optional<Long> getThreshold() {
        return threshold;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.TOP_K;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitTopK(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopK that = (TopK) o;
        return Objects.equals(columns, that.columns) &&
                Objects.equals(size, that.size) &&
                Objects.equals(threshold, that.threshold);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, size, threshold);
    }
}
