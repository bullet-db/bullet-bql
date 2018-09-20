/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;
import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.Distribution;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.GroupBy;
import com.yahoo.bullet.bql.tree.GroupingElement;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.LongLiteral;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.bql.tree.Select;
import com.yahoo.bullet.bql.tree.SelectItem;
import com.yahoo.bullet.bql.tree.SimpleGroupBy;
import com.yahoo.bullet.bql.tree.Stream;
import com.yahoo.bullet.bql.tree.TopK;
import com.yahoo.bullet.bql.tree.WindowInclude;
import com.yahoo.bullet.bql.tree.WindowInclude.IncludeType;
import com.yahoo.bullet.bql.tree.Windowing;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Clause.Operation;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.AVG;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MAX;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MIN;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.SUM;
import static com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType.GROUP;
import static com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType.SELECT_FIELDS;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.ALL;
import static com.yahoo.bullet.bql.tree.SelectItem.Type.COMPUTATION;
import static java.util.Objects.requireNonNull;

@Slf4j
public class QueryExtractor {
    private final ExtractVisitor extractVisitor = new ExtractVisitor();
    private final Long queryMaxDuration;

    private Set<Expression> groupByFields;
    private Set<Expression> selectFields;
    private Set<Expression> computations;
    private Map<Node, Identifier> aliases;
    private Optional<Long> size;
    private Optional<Long> threshold;
    private Long duration;
    private List<Clause> filters;
    private Window window;
    private Aggregation aggregation;
    private Projection projection;
    private List<PostAggregation> postAggregations;
    private QueryType type;
    /**
     * The constructor with a {@link BQLConfig}.
     *
     * @param bqlConfig A {@link BQLConfig} for parsing BQL statement.
     */
    public QueryExtractor(BQLConfig bqlConfig) {
        queryMaxDuration = bqlConfig.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);
    }

    /**
     * Validate a {@link com.yahoo.bullet.bql.tree.Statement} tree and return a {@link Query}.
     *
     * @param node The non-null root {@link Node} of the tree.
     * @param type The {@link QueryType} of the BQL statement.
     * @return A {@link Query}.
     * @throws ParsingException     when BQL statement is not valid.
     * @throws NullPointerException when node is null.
     */
    public Query validateAndExtract(Node node, QueryType type) throws ParsingException, NullPointerException {
        requireNonNull(node, "Cannot validate and extract Bullet query from null");

        reset();
        this.type = type;
        extractVisitor.process(node);
        return constructQuery();
    }

    private void reset() {
        groupByFields = new HashSet<>();
        selectFields = new HashSet<>();
        computations = new HashSet<>();
        aliases = new HashMap<>();
        size = Optional.empty();
        threshold = Optional.empty();
        duration = null;
        filters = null;
        window = null;
        aggregation = null;
        projection = null;
        postAggregations = null;
    }

    private Query constructQuery() {
        Query query = new Query();
        query.setAggregation(aggregation);
        query.setDuration(duration);
        query.setFilters(filters);
        query.setProjection(projection);
        query.setWindow(window);
        if (postAggregations != null && !postAggregations.isEmpty()) {
            query.setPostAggregations(postAggregations);
        }
        return query;
    }

    // A private visitor to visit node tree and validate, extract query information.
    private class ExtractVisitor extends DefaultTraversalVisitor<Void, Void> {
        @Override
        protected Void visitQuery(com.yahoo.bullet.bql.tree.Query node, Void context) throws ParsingException {
            process(node.getQueryBody());
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Void context) throws ParsingException {
            process(node.getSelect());
            visitLimit(node.getLimit());
            switch (type) {
                case TOP_K:
                    extractTopK(node);
                    break;
                case TOP_K_FUNCTION:
                    extractTopKFunction(node);
                    break;
                case DISTRIBUTION:
                    extractDistribution(node);
                    break;
                case COUNT_DISTINCT:
                    extractCountDistinct(node);
                    break;
                case GROUP:
                case DISTINCT_SELECT:
                    extractGroup(node);
                    break;
                case SELECT_FIELDS:
                case SELECT_ALL:
                    extractRaw();
                    node.getOrderBy().ifPresent(value -> {
                            postAggregations.add(new OrderByExtractor(value).extractOrderBy());
                        });
                    break;
                case UNKNOWN:
                    throw new ParsingException("BQL cannot be classified");
            }

            node.getFrom().ifPresent(this::process);
            node.getWhere().ifPresent(value -> {
                    process(value);
                    filters = new FilterExtractor().extractFilter(node);
                });
            node.getWindowing().ifPresent(value -> {
                    process(value);
                    window = new WindowExtractor(value).extractWindow();
                });
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Void context) throws ParsingException {
            List<SelectItem> selectItems = node.getSelectItems();
            for (SelectItem item : selectItems) {
                process(item);
                updateSelectAndAliases(item);
            }
            return null;
        }

        @Override
        protected Void visitStream(Stream node, Void context) throws ParsingException {
            if (node.getRecordDuration().isPresent()) {
                throw new ParsingException("Stream duration control based on record is not supported yet");
            }

            Optional<String> timeDuration = node.getTimeDuration();
            if (timeDuration.isPresent()) {
                if (timeDuration.get().equalsIgnoreCase("MAX")) {
                    log.debug("Used query max duration (ms): " + queryMaxDuration);
                    duration = queryMaxDuration;
                } else {
                    duration = Long.parseLong(timeDuration.get());
                }
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
        protected Void visitSimpleGroupBy(SimpleGroupBy node, Void context) {
            groupByFields = new HashSet<>(node.getColumnExpressions());
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context) throws ParsingException {
            GroupOperationType type = node.getType();
            List<Expression> arguments = node.getArguments();
            Boolean isDistinct = node.isDistinct();
            validateFunctionField(type, arguments, isDistinct);
            return null;
        }

        @Override
        protected Void visitWindowing(Windowing node, Void context) {
            process(node.getInclude());
            return null;
        }

        @Override
        protected Void visitWindowInclude(WindowInclude node, Void context) throws ParsingException {
            if (node.getType().isPresent() && node.getType().get() == IncludeType.LAST) {
                throw new ParsingException("WINDOWING doesn't support last include yet");
            }

            return null;
        }

        private void visitLimit(Optional<String> limit) throws ParsingException {
            if (limit.isPresent() && limit.get().equalsIgnoreCase("ALL")) {
                throw new ParsingException("LIMIT ALL is not supported yet");
            }

            size = limit.map(Long::parseLong);
        }

        private void visitHaving(Optional<Expression> having) {
            having.ifPresent(expression -> {
                    Expression right = ((ComparisonExpression) expression).getRight();
                    long rightValue = ((LongLiteral) right).getValue();
                    Operation op = ((ComparisonExpression) expression).getOperation();
                    switch (op) {
                        case GREATER_EQUALS:
                            threshold = Optional.of(rightValue);
                            break;
                        case GREATER_THAN:
                            threshold = Optional.of(rightValue + 1);
                            break;
                        default:
                            throw new ParsingException("Only > or >= are supported in HAVING clause");
                    }
                });
        }

        private void updateSelectAndAliases(SelectItem item) {
            if (item.getType() == ALL) {
                return;
            }
            if (item.getType() == COMPUTATION) {
                computations.add(item.getValue());
            } else {
                selectFields.add(item.getValue());
            }
            item.getAlias().ifPresent(alias -> aliases.put(item.getValue(), alias));
        }

        private void validateFunctionField(GroupOperationType type, List<Expression> arguments, boolean isDistinct) throws ParsingException {
            if (type == COUNT && !arguments.isEmpty() && !isDistinct) {
                throw new ParsingException("COUNT(*) does't support field");
            }

            if (type == MIN || type == MAX || type == AVG || type == SUM) {
                if (isDistinct) {
                    throw new ParsingException(type + " function doesn't support DISTINCT");
                }
                if (arguments.size() != 1) {
                    throw new ParsingException(type + " function requires only 1 field");
                }
            }
        }

        private void extractTopK(QuerySpecification node) {
            process(node.getGroupBy().get());
            visitHaving(node.getHaving());
            aggregation = new AggregationExtractor(aliases).extractTopK(groupByFields, threshold, size);
        }

        private void extractTopKFunction(QuerySpecification node) throws ParsingException {
            SelectItem item = node.getSelect().getSelectItems().get(0);
            TopK topK = (TopK) item.getValue();
            if (size.isPresent() && !size.get().equals(topK.getSize())) {
                throw new ParsingException("LIMIT must be same as the k of TopK aggregation");
            }

            aggregation = new AggregationExtractor(aliases).extractTopKFunction(topK);
        }

        private void extractDistribution(QuerySpecification node) {
            SelectItem item = node.getSelect().getSelectItems().get(0);
            Distribution distribution = (Distribution) item.getValue();
            aggregation = new AggregationExtractor(aliases).extractDistribution(distribution, size);
        }

        private void extractCountDistinct(QuerySpecification node) throws ParsingException {
            SelectItem item = node.getSelect().getSelectItems().get(0);
            if (size.isPresent()) {
                throw new ParsingException("LIMIT is not supported for CountDistinct aggregation");
            }

            FunctionCall countDistinct = (FunctionCall) item.getValue();
            aggregation = new AggregationExtractor(aliases).extractCountDistinct(countDistinct);
        }

        private void extractGroup(QuerySpecification node) {
            if (type == GROUP && !node.getGroupBy().isPresent()) {
                groupByFields = new HashSet<>();
            } else if (type == GROUP) {
                process(node.getGroupBy().get());
            } else {
                groupByFields = selectFields;
            }
            aggregation = new AggregationExtractor(aliases).extractGroup(selectFields, groupByFields, size);
        }

        private void extractRaw() {
            aggregation = new AggregationExtractor(aliases).extractRaw(size);
            postAggregations = new ComputationExtractor(computations, aliases).extractComputations();
            if (type == SELECT_FIELDS) {
                projection = new ProjectionExtractor(selectFields, aliases).extractProjection();
            }
        }
    }
}
