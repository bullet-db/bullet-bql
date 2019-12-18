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

import static com.yahoo.bullet.aggregations.Distribution.NUMBER_OF_POINTS;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;

@Getter
public class LinearDistributionNode extends DistributionNode {
    private final Long numberOfPoints;

    public LinearDistributionNode(Type type, ExpressionNode expression, Long numberOfPoints) {
        super(type, expression);
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TYPE, type);
        attributes.put(NUMBER_OF_POINTS, numberOfPoints);
        return attributes;
    }

    @Override
    public String attributesToString() {
        return getDistributionType(type) + "(" + expression.toFormatlessString() + ", LINEAR, " + numberOfPoints + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LinearDistributionNode)) {
            return false;
        }
        LinearDistributionNode other = (LinearDistributionNode) obj;
        return type == other.type &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(numberOfPoints, other.numberOfPoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, expression, numberOfPoints);
    }
}
