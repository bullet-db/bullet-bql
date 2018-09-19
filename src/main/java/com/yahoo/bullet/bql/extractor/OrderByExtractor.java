package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class OrderByExtractor {
    private OrderBy node;

    /**
     * Constructor that requires an {@link OrderBy}.
     *
     * @param node A non-null {@link OrderBy}.
     * @throws NullPointerException when node is null.
     */
    public OrderByExtractor(OrderBy node) throws NullPointerException {
        requireNonNull(node);

        this.node = node;
    }

    public PostAggregation extractOrderBy() {
        List<String> fields = node.getSortItems().stream().map(sortItem -> sortItem.getSortKey().toFormatlessString()).collect(Collectors.toList());
        com.yahoo.bullet.parsing.OrderBy.Direction direction =
                node.getOrdering() == OrderBy.Ordering.ASCENDING ?
                        com.yahoo.bullet.parsing.OrderBy.Direction.ASC :
                        com.yahoo.bullet.parsing.OrderBy.Direction.DESC;
        com.yahoo.bullet.parsing.OrderBy orderBy = new com.yahoo.bullet.parsing.OrderBy();
        orderBy.setType(PostAggregation.Type.ORDER_BY);
        orderBy.setFields(fields);
        orderBy.setDirection(direction);
        return orderBy;
    }
}
