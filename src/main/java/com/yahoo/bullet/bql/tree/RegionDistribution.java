/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.Distribution.RANGE_END;
import static com.yahoo.bullet.aggregations.Distribution.RANGE_INCREMENT;
import static com.yahoo.bullet.aggregations.Distribution.RANGE_START;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;
import static java.util.Objects.requireNonNull;

public class RegionDistribution extends Distribution {
    private final Double start;
    private final Double end;
    private final Double increment;

    /**
     * Constructor that requires a List of {@link Expression} columns, a {@link Type} and a Double start, a Double end and a Double increment.
     *
     * @param columns   A List of {@link Expression}.
     * @param type      A {@link Type}.
     * @param start     A Double.
     * @param end       A Double.
     * @param increment A Double.
     */
    public RegionDistribution(List<Expression> columns, Type type, Double start, Double end, Double increment) {
        this(Optional.empty(), columns, type, start, end, increment);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link Expression} columns, a {@link Type} and a Double start, a Double end and a Double increment.
     *
     * @param location  A {@link NodeLocation}.
     * @param columns   A List of {@link Expression}.
     * @param type      A {@link Type}.
     * @param start     A Double.
     * @param end       A Double.
     * @param increment A Double.
     */
    public RegionDistribution(NodeLocation location, List<Expression> columns, Type type, Double start,
                              Double end, Double increment) {
        this(Optional.of(location), columns, type, start, end, increment);
    }

    private RegionDistribution(Optional<NodeLocation> location, List<Expression> columns, Type type, Double start, Double end, Double increment) {
        super(location, columns, type);
        this.start = requireNonNull(start, "start is null");
        this.end = requireNonNull(end, "end is null");
        this.increment = requireNonNull(increment, "increment is null");
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TYPE, type);
        attributes.put(RANGE_START, start);
        attributes.put(RANGE_END, end);
        attributes.put(RANGE_INCREMENT, increment);
        return attributes;
    }

    @Override
    public String attributesToString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getDistributionType(type))
                .append("(")
                .append(columns.get(0).toFormatlessString())
                .append(", REGION, ")
                .append(start)
                .append(", ")
                .append(end)
                .append(", ")
                .append(increment)
                .append(")");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegionDistribution that = (RegionDistribution) o;
        return Objects.equals(columns, that.columns) && Objects.equals(type, that.type)
                && Objects.equals(start, that.start) && Objects.equals(end, that.end)
                && Objects.equals(increment, that.increment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, type, start, end, increment);
    }
}
