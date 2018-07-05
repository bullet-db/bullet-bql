/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.parsing.Window.Unit;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class WindowInclude extends Node {
    public enum IncludeType {
        FIRST,
        LAST
    }

    private final Unit unit;
    private final Optional<IncludeType> type;
    private final Optional<Long> number;

    /**
     * Constructor that requires an {@link Unit}, an Optional of {@link IncludeType} and an Optional of Long.
     *
     * @param unit   An {@link Unit}.
     * @param type   An Optional of {@link IncludeType}.
     * @param number An Optional of Long.
     */
    public WindowInclude(Unit unit, Optional<IncludeType> type, Optional<Long> number) {
        this(Optional.empty(), unit, type, number);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Unit}, an Optional of {@link IncludeType} and an Optional of Long.
     *
     * @param location A {@link NodeLocation}.
     * @param unit     An {@link Unit}.
     * @param type     An Optional of {@link IncludeType}.
     * @param number   An Optional of Long.
     */
    public WindowInclude(NodeLocation location, Unit unit, Optional<IncludeType> type, Optional<Long> number) {
        this(Optional.of(location), unit, type, number);
    }

    private WindowInclude(Optional<NodeLocation> location, Unit unit, Optional<IncludeType> type, Optional<Long> number) {
        super(location);
        this.type = requireNonNull(type, "includeType is null");
        this.unit = requireNonNull(unit, "unit is null");
        this.number = requireNonNull(number, "number is null");
    }

    /**
     * Get the {@link #type} of this WindowInclude.
     *
     * @return An Optional of {@link IncludeType}.
     */
    public Optional<IncludeType> getType() {
        return type;
    }

    /**
     * Get the {@link #unit} of this WindowInclude.
     *
     * @return An {@link Unit}.
     */
    public Unit getUnit() {
        return unit;
    }

    /**
     * Get the {@link #number} of this WindowInclude.
     *
     * @return An Optional of Long.
     */
    public Optional<Long> getNumber() {
        return number;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindowInclude(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowInclude o = (WindowInclude) obj;
        return Objects.equals(type, o.type) &&
                Objects.equals(unit, o.unit) &&
                Objects.equals(number, o.number);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, unit, number);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("type", type)
                .add("unit", unit)
                .add("number", number)
                .omitNullValues()
                .toString();
    }
}
