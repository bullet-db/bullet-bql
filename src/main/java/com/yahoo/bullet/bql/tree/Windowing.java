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

public class Windowing extends Node {
    private final Long emitEvery;
    private final Unit emitType;
    private final WindowInclude include;

    /**
     * Constructor that requires a Long, a {@link Unit} and a {@link WindowInclude}.
     *
     * @param emitEvery A Long.
     * @param emitType  A {@link Unit}.
     * @param include   A {@link WindowInclude}.
     */
    public Windowing(Long emitEvery, Unit emitType, WindowInclude include) {
        this(Optional.empty(), emitEvery, emitType, include);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a Long, a {@link Unit} and a {@link WindowInclude}.
     *
     * @param location  A {@link NodeLocation}.
     * @param emitEvery A Long.
     * @param emitType  A {@link Unit}.
     * @param include   A {@link WindowInclude}.
     */
    public Windowing(NodeLocation location, Long emitEvery, Unit emitType, WindowInclude include) {
        this(Optional.of(location), emitEvery, emitType, include);
    }

    private Windowing(Optional<NodeLocation> location, Long emitEvery, Unit emitType, WindowInclude include) {
        super(location);
        this.emitEvery = requireNonNull(emitEvery, "emitEvery is null");
        this.emitType = requireNonNull(emitType, "emitType is null");
        this.include = requireNonNull(include, "include is null");
    }

    /**
     * Get the {@link #emitEvery} of this Windowing.
     *
     * @return A Long.
     */
    public Long getEmitEvery() {
        return emitEvery;
    }

    /**
     * Get the {@link #emitType} of this Windowing.
     *
     * @return A {@link Unit}.
     */
    public Unit getEmitType() {
        return emitType;
    }

    /**
     * Get the {@link #include} of this Windowing.
     *
     * @return A {@link WindowInclude}.
     */
    public WindowInclude getInclude() {
        return include;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWindowing(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("emitEvery", emitEvery)
                .add("emitType", emitType)
                .add("include", include)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Windowing o = (Windowing) obj;
        return Objects.equals(emitEvery, o.emitEvery) &&
                Objects.equals(emitType, o.emitType) &&
                Objects.equals(include, o.include);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emitEvery, emitType, include);
    }
}
