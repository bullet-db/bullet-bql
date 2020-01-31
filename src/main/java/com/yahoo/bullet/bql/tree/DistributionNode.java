/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;
import com.yahoo.bullet.bql.parser.ParsingException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public abstract class DistributionNode extends ExpressionNode {
    public static final String QUANTILE = "QUANTILE";
    public static final String FREQ = "FREQ";
    public static final String CUMFREQ = "CUMFREQ";

    protected final Type type;
    @Getter
    protected final ExpressionNode expression;

    /**
     * Get the attributes of this DistributionNode.
     *
     * @return A Map of String and Object that represents attributes.
     */
    public abstract Map<String, Object> getAttributes();

    /**
     * Convert attributes to BQL expression.
     *
     * @return A String.
     */
    public abstract String attributesToString();

    /**
     * Get the BQL String of this Distribution's type.
     *
     * @return A BQL String that represents type.
     */
    protected String getDistributionType() {
        switch (type) {
            case QUANTILE:
                return QUANTILE;
            case PMF:
                return FREQ;
            case CDF:
                return CUMFREQ;
        }
        // Unreachable
        throw new ParsingException("Unknown distribution type");
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDistribution(this, context);
    }
}
