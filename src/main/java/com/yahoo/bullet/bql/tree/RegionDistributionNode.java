/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.yahoo.bullet.aggregations.Distribution.RANGE_END;
import static com.yahoo.bullet.aggregations.Distribution.RANGE_INCREMENT;
import static com.yahoo.bullet.aggregations.Distribution.RANGE_START;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;

@Getter
public class RegionDistributionNode extends DistributionNode {
    private final Double start;
    private final Double end;
    private final Double increment;

    public RegionDistributionNode(Type type, ExpressionNode expression, Double start, Double end, Double increment) {
        super(type, expression);
        this.start = start;
        this.end = end;
        this.increment = increment;
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
        return getDistributionType() + "(" + expression.getName() + ", REGION, " + start + ", " + end + ", " + increment + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof RegionDistributionNode)) {
            return false;
        }
        RegionDistributionNode other = (RegionDistributionNode) obj;
        return type == other.type &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(start, other.start) &&
               Objects.equals(end, other.end) &&
               Objects.equals(increment, other.increment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, expression, start, end, increment);
    }
}
