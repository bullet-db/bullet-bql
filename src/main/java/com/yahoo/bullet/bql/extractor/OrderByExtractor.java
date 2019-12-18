/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.parsing.PostAggregation;

import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.OrderBy.Direction;
import static java.util.Objects.requireNonNull;

public class OrderByExtractor {
    private OrderByNode node;

    /**
     * Constructor that requires an {@link OrderByNode}.
     *
     * @param node A non-null {@link OrderByNode}.
     */
    public OrderByExtractor(OrderByNode node) {
        requireNonNull(node);
        this.node = node;
    }

    /**
     * Extract an OrderByNode PostAggregation.
     *
     * @return A PostAggregation based on an OrderByNode node
     */
    public PostAggregation extractOrderBy() {
        List<com.yahoo.bullet.parsing.OrderBy.SortItem> fields = node.getSortItems().stream().map(sortItem ->
                new com.yahoo.bullet.parsing.OrderBy.SortItem(
                        sortItem.getSortKey().toFormatlessString(),
                        sortItem.getOrdering() == SortItemNode.Ordering.DESCENDING ? Direction.DESC : Direction.ASC)
            ).collect(Collectors.toList());
        com.yahoo.bullet.parsing.OrderBy orderBy = new com.yahoo.bullet.parsing.OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(fields);
        return orderBy;
    }
}
