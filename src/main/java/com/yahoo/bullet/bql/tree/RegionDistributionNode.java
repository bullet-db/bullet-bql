/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.RegionDistribution;

import java.util.Objects;

public class RegionDistributionNode extends DistributionNode {
    private final double start;
    private final double end;
    private final double increment;

    /**
     * Constructs a RegionDistributionNode from a {@link DistributionType}, {@link ExpressionNode}, {@link Double} start,
     * a {@link Double} end, and {@link Double} increment.
     *
     * @param type The distribution type.
     * @param expression The distribution variable.
     * @param start The start of the range.
     * @param end The end of the range.
     * @param increment The interval between points.
     * @param nodeLocation The location of the node.
     */
    public RegionDistributionNode(DistributionType type, ExpressionNode expression, double start, double end, double increment, NodeLocation nodeLocation) {
        super(type, expression, nodeLocation);
        this.start = start;
        this.end = end;
        this.increment = increment;
    }

    @Override
    public Distribution getAggregation(Integer size) {
        return new RegionDistribution(expression.getName(), type, size, start, end, increment);
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

    @Override
    public String toString() {
        return type.getName() + "(" + expression.getName() + ", REGION, " + start + ", " + end + ", " + increment + ")";
    }
}
