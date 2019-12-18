/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@Getter
@RequiredArgsConstructor
public abstract class DistributionNode extends ExpressionNode {
    protected final Type type;
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
     * @param type A {@link Type}.
     * @return A BQL String that represents type.
     * @throws UnsupportedOperationException when Distribution's type is not supported.
     */
    protected String getDistributionType(Type type) {
        switch (type) {
            case QUANTILE:
                return "QUANTILE";
            case PMF:
                return "FREQ";
            case CDF:
                return "CUMFREQ";
        }
        throw new UnsupportedOperationException("Distribution type is not supported");
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDistribution(this, context);
    }
}
