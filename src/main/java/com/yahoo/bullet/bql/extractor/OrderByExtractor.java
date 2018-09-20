/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;

import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bullet.bql.tree.OrderBy.Ordering;
import static com.yahoo.bullet.parsing.OrderBy.Direction;
import static java.util.Objects.requireNonNull;

public class OrderByExtractor {
    private OrderBy node;

    /**
     * Constructor that requires an {@link OrderBy}.
     *
     * @param node A non-null {@link OrderBy}.
     */
    public OrderByExtractor(OrderBy node) {
        requireNonNull(node);
        this.node = node;
    }

    /**
     * Extract an OrderBy PostAggregation.
     *
     * @return A PostAggregation based on an OrderBy node
     */
    public PostAggregation extractOrderBy() {
        List<String> fields = node.getSortItems().stream().map(sortItem -> sortItem.getSortKey().toFormatlessString()).collect(Collectors.toList());
        com.yahoo.bullet.parsing.OrderBy.Direction direction =
                node.getOrdering() == Ordering.ASCENDING ? Direction.ASC : Direction.DESC;
        com.yahoo.bullet.parsing.OrderBy orderBy = new com.yahoo.bullet.parsing.OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(fields);
        orderBy.setDirection(direction);
        return orderBy;
    }
}
