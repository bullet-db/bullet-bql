/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.GroupBy;
import com.yahoo.bullet.bql.tree.GroupingElement;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.bql.tree.Select;
import com.yahoo.bullet.bql.tree.SelectItem;
import com.yahoo.bullet.bql.tree.SelectItem.Type;
import com.yahoo.bullet.bql.tree.SimpleGroupBy;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.ALL;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.COLUMN;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.DISTRIBUTION;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.GROUP;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.SUB_ALL;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.SUB_COLUMN;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.TOP_K;
import static java.util.Objects.requireNonNull;

public class QueryClassifier {
    /**
     * Represents the type of BQL query statement.
     */
    public enum QueryType {
        // TopK formed by BQL statement.
        TOP_K,

        // TopK formed by TopK(k, threshold, fields) function.
        TOP_K_FUNCTION,

        // QUANTILE, FREQ, CUMFREQ
        DISTRIBUTION,

        // COUNT(DISTINCT fields)
        COUNT_DISTINCT,

        // COUNT(*), MIN(fields), MAX(fields), SUM(fields), AVG(fields)
        GROUP,

        // SELECT DISTINCT field FROM STREAM(n, TIME) GROUP BY field
        DISTINCT_SELECT,

        // SELECT field
        SELECT_FIELDS,

        // SELECT *
        SELECT_ALL,

        UNKNOWN
    }

    private final ClassifyVisitor visitor = new ClassifyVisitor();
    private Set<Expression> groupByFields;
    private Set<Expression> selectFields;
    private QueryType type;

    /**
     * Classify a BQL by traversing node tree.
     *
     * @param node The non-null root {@link Node} of the tree.
     * @return A {@link QueryType}.
     * @throws ParsingException     when BQL statement is not valid.
     * @throws NullPointerException when node is null.
     */
    public QueryType classifyQuery(Node node) throws ParsingException, NullPointerException {
        requireNonNull(node, "Cannot classify query from null");

        groupByFields = new HashSet<>();
        selectFields = new HashSet<>();
        type = QueryType.UNKNOWN;
        visitor.process(node);
        return type;
    }

    // A private visitor to visit node tree and classify BQL statement.
    private class ClassifyVisitor extends DefaultTraversalVisitor<Void, Void> {
        @Override
        protected Void visitQuery(com.yahoo.bullet.bql.tree.Query node, Void context) throws ParsingException {
            process(node.getQueryBody());
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Void context) throws ParsingException {
            if (isTopK(node)) {
                type = QueryType.TOP_K;
                return null;
            } else if (node.getOrderBy().isPresent() || node.getHaving().isPresent()) {
                throw new ParsingException("ORDER BY or HAVING are only supported for TOP K");
            }

            process(node.getSelect());
            node.getGroupBy().ifPresent(this::process);
            if (type == QueryType.GROUP && !node.getGroupBy().isPresent()) {
                throw new ParsingException("Grouping functions must be followed by GROUP BY () or GROUP BY element (, element)*");
            }

            if (type == QueryType.UNKNOWN) {
                type = QueryType.SELECT_FIELDS;
            }
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Void context) throws ParsingException {
            if (node.isDistinct()) {
                type = QueryType.DISTINCT_SELECT;
            }
            List<SelectItem> selectItems = node.getSelectItems();

            for (SelectItem item : selectItems) {
                Type selectType = item.getType();

                if (!isValidSelectDistinct(selectType)) {
                    throw new ParsingException("SELECT DISTINCT can only run with field, field.subFiled or field.*");
                }

                if ((selectType == COUNT_DISTINCT || selectType == TOP_K || selectType == DISTRIBUTION ||
                        selectType == ALL) && selectItems.size() > 1) {
                    throw new ParsingException("SELECT *, TOP_K, DISTRIBUTION, COUNT DISTINCT cannot run with other selectItems");
                }
                updateSelectFields(item);
                updateAggregation(item);
            }
            return null;
        }

        @Override
        protected Void visitGroupBy(GroupBy node, Void context) {
            for (GroupingElement groupingElement : node.getGroupingElements()) {
                process(groupingElement);
            }
            return null;
        }

        @Override
        protected Void visitSimpleGroupBy(SimpleGroupBy node, Void context) throws ParsingException {
            groupByFields = new HashSet<>(node.getColumnExpressions());

            if (type == QueryType.TOP_K_FUNCTION || type == QueryType.DISTRIBUTION || type == QueryType.COUNT_DISTINCT || type == QueryType.SELECT_ALL) {
                throw new ParsingException("NonGroup aggregation cannot be followed by GROUP BY");
            }

            if (type == QueryType.DISTINCT_SELECT && !groupByFields.equals(selectFields)) {
                throw new ParsingException("SELECT DISTINCT contains fields which are not GROUP BY fields");
            }

            validateSelectWithGroupBy();
            // Update hasGroup, hasNonRaw when SELECT field1, field2 GROUP BY field1, field2.
            type = QueryType.GROUP;
            return null;
        }

        private boolean isTopK(QuerySpecification node) {
            if (!hasRequiredTopKClauses(node)) {
                return false;
            } else {
                Set<Expression> topKGroupByFields = getTopKGroupByFields(node);
                Set<Expression> topKSelectFields = getTopKSelectFields(node);

                if (node.getLimit().get().equalsIgnoreCase("ALL")) {
                    return false;
                }

                if (topKSelectFields.isEmpty()) {
                    return false;
                }

                return topKGroupByFields.equals(topKSelectFields);
            }
        }

        private boolean hasRequiredTopKClauses(QuerySpecification node) {
            return node.getGroupBy().isPresent() && node.getOrderBy().isPresent() && node.getLimit().isPresent();
        }

        private Set<Expression> getTopKGroupByFields(QuerySpecification node) {
            SimpleGroupBy simpleGroupBy = (SimpleGroupBy) node.getGroupBy().get().getGroupingElements().get(0);
            return new HashSet<>(simpleGroupBy.getColumnExpressions());
        }

        private Set<Expression> getTopKSelectFields(QuerySpecification node) throws ParsingException {
            List<SelectItem> selectItems = node.getSelect().getSelectItems();
            Set<Expression> topKSelectFields = new HashSet<>();
            for (SelectItem selectItem : selectItems) {
                Type type = selectItem.getType();

                // Use emptyList to represent invalid select field.
                if (!isValidTopKSelectField(selectItem)) {
                    return Collections.emptySet();
                }

                if (type != GROUP) {
                    topKSelectFields.add(selectItem.getValue());
                }
            }

            if (selectItems.size() - topKSelectFields.size() > 1) {
                throw new ParsingException("For Top K, there can only be one COUNT(*)");
            }

            return topKSelectFields;
        }

        private boolean isValidTopKSelectField(SelectItem selectItem) {
            Type type = selectItem.getType();
            if (type != COLUMN && type != SUB_ALL && type != SUB_COLUMN && type != GROUP) {
                return false;
            }

            return type != GROUP || ((FunctionCall) selectItem.getValue()).getType() == COUNT;
        }

        private boolean isValidSelectDistinct(Type selectType) {
            return type != QueryType.DISTINCT_SELECT || selectType == COLUMN || selectType == SUB_COLUMN || selectType == SUB_ALL;
        }

        private void updateSelectFields(SelectItem item) {
            if (item.getType() == ALL) {
                return;
            }

            selectFields.add(item.getValue());
        }

        private void updateAggregation(SelectItem item) {
            switch (item.getType()) {
                case ALL:
                    type = QueryType.SELECT_ALL;
                    break;
                case GROUP:
                    type = QueryType.GROUP;
                    break;
                case TOP_K:
                    type = QueryType.TOP_K_FUNCTION;
                    break;
                case DISTRIBUTION:
                    type = QueryType.DISTRIBUTION;
                    break;
                case COUNT_DISTINCT:
                    type = QueryType.COUNT_DISTINCT;
                    break;
                default:
            }
        }

        private void validateSelectWithGroupBy() throws ParsingException {
            for (Expression field : selectFields) {
                Type selectType = field.getType(Type.class);

                // For group all, selectItem must be group function.
                // For group by, nonGroupFunction selectItems must be the same as groupingElements.
                if (groupByFields.isEmpty() && selectType != GROUP) {
                    throw new ParsingException("GROUP BY () only supports grouping functions as selectItems");
                } else if (!groupByFields.isEmpty() && selectType != GROUP && !(groupByFields.contains(field) && selectFields.containsAll(groupByFields))) {
                    throw new ParsingException("GROUP BY element (, element)* only supports grouping elements or grouping functions as selectItems");
                }
            }
        }
    }
}
