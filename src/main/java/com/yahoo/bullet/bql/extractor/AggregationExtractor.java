/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.Distribution;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.TopK;
import com.yahoo.bullet.parsing.Aggregation;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.TopK.NEW_NAME_FIELD;
import static com.yahoo.bullet.aggregations.TopK.THRESHOLD_FIELD;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATIONS;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATION_FIELD;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATION_TYPE;
import static com.yahoo.bullet.parsing.Aggregation.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.parsing.Aggregation.Type.DISTRIBUTION;
import static com.yahoo.bullet.parsing.Aggregation.Type.GROUP;
import static com.yahoo.bullet.parsing.Aggregation.Type.TOP_K;
import static java.util.Objects.requireNonNull;

@Slf4j
public class AggregationExtractor {
    private Map<Node, Identifier> aliases;

    /**
     * Constructor that requires aliases.
     *
     * @param aliases The map of {@link Identifier}.
     */
    public AggregationExtractor(Map<Node, Identifier> aliases) {
        this.aliases = aliases;
    }

    /**
     * Extract a {@link Aggregation.Type#TOP_K} Aggregation.
     *
     * @param groupByFields The non-null Set of {@link Expression} in the BQL GROUP BY clause.
     * @param threshold     The non-null Optional of threshold of this TopK aggregation.
     * @param size          The non-null Optional of size of this TopK aggregation.
     * @return A {@link Aggregation.Type#TOP_K} Aggregation.
     * @throws NullPointerException when any of groupByFields, threshold and size is null.
     */
    public Aggregation extractTopK(Set<Expression> groupByFields, Optional<Long> threshold, Optional<Long> size) throws NullPointerException {
        requireNonNull(groupByFields);
        requireNonNull(threshold);
        requireNonNull(size);

        Aggregation topK = new Aggregation();
        topK.setType(TOP_K);
        size.ifPresent(sizeValue -> topK.setSize(sizeValue.intValue()));
        topK.setFields(getFields(new ArrayList<>(groupByFields)));

        Map<String, Object> attributes = new HashMap<>();
        threshold.ifPresent(min -> attributes.put(THRESHOLD_FIELD, min));
        for (Node node : aliases.keySet()) {
            if (node instanceof FunctionCall) {
                attributes.put(NEW_NAME_FIELD, getAlias((Expression) node));
            }
        }
        if (!attributes.isEmpty()) {
            topK.setAttributes(attributes);
        }
        return topK;
    }

    /**
     * Extract a {@link Aggregation.Type#TOP_K} Aggregation.
     *
     * @param node The non-null {@link TopK} expression.
     * @return A {@link Aggregation.Type#TOP_K} Aggregation.
     * @throws NullPointerException when node is null.
     */
    public Aggregation extractTopKFunction(TopK node) throws NullPointerException {
        requireNonNull(node);

        Aggregation topK = new Aggregation();
        topK.setType(TOP_K);
        topK.setSize(node.getSize().intValue());
        topK.setFields(getFields(node.getColumns()));

        Map<String, Object> attributes = new HashMap<>();
        node.getThreshold().ifPresent(threshold -> attributes.put(THRESHOLD_FIELD, threshold));
        if (aliases.containsKey(node)) {
            attributes.put(NEW_NAME_FIELD, getAlias(node));
        }
        if (!attributes.isEmpty()) {
            topK.setAttributes(attributes);
        }
        return topK;
    }

    /**
     * Extract a {@link Aggregation.Type#DISTRIBUTION} Aggregation.
     *
     * @param node The non-null {@link Distribution} expression.
     * @param size The non-null Optional of size of this distribution aggregation.
     * @return A {@link Aggregation.Type#DISTRIBUTION} Aggregation.
     * @throws NullPointerException when any of node and size is null.
     */
    public Aggregation extractDistribution(Distribution node, Optional<Long> size) throws NullPointerException {
        requireNonNull(node);
        requireNonNull(size);

        Aggregation distribution = new Aggregation();
        distribution.setType(DISTRIBUTION);
        size.ifPresent(sizeValue -> distribution.setSize(sizeValue.intValue()));
        distribution.setFields(getFields(node.getColumns()));

        Map<String, Object> attributes = node.getAttributes();
        if (aliases.containsKey(node)) {
            attributes.put(NEW_NAME_FIELD, getAlias(node));
        }

        distribution.setAttributes(attributes);
        return distribution;
    }

    /**
     * Extract a {@link Aggregation.Type#COUNT_DISTINCT} Aggregation.
     *
     * @param node The non-null COUNT_DISTINCT {@link FunctionCall} expression.
     * @return A {@link Aggregation.Type#COUNT_DISTINCT} Aggregation.
     * @throws NullPointerException when node is null.
     */
    public Aggregation extractCountDistinct(FunctionCall node) throws NullPointerException {
        requireNonNull(node);

        Aggregation countDistinct = new Aggregation();
        countDistinct.setType(COUNT_DISTINCT);
        countDistinct.setFields(getFields(node.getArguments()));

        if (aliases.containsKey(node)) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(NEW_NAME_FIELD, getAlias(node));
            countDistinct.setAttributes(attributes);
        }
        return countDistinct;
    }

    /**
     * Extract a {@link Aggregation.Type#GROUP} Aggregation.
     *
     * @param selectFields  The non-null Set of {@link Expression} in the BQL SELECT clause.
     * @param groupByFields The non-null Set of {@link Expression} in the BQL GROUP BY clause.
     * @param size          The non-null Optional of size of this group aggregation.
     * @return A {@link Aggregation.Type#GROUP} Aggregation.
     * @throws NullPointerException when any of selectFields, groupByFields and size is null.
     * @throws ParsingException when any of selectionField is not grouping function nor in group by clause.
     */
    public Aggregation extractGroup(Set<Expression> selectFields, Set<Expression> groupByFields, Optional<Long> size) throws NullPointerException, ParsingException {
        requireNonNull(selectFields);
        requireNonNull(groupByFields);
        requireNonNull(size);

        Aggregation group = new Aggregation();
        group.setType(GROUP);
        size.ifPresent(sizeValue -> group.setSize(sizeValue.intValue()));
        if (!groupByFields.isEmpty()) {
            group.setFields(getFields(new ArrayList<>(groupByFields)));
        }

        Map<String, Object> attributes = getGroupAttributes(selectFields, groupByFields);
        if (!attributes.isEmpty()) {
            group.setAttributes(attributes);
        }
        return group;
    }

    /**
     * Extract a {@link Aggregation.Type#RAW} Aggregation.
     *
     * @param size The non-null size from BQL LIMIT clause.
     * @return A {@link Aggregation.Type#RAW} Aggregation.
     * @throws NullPointerException when size is null.
     */
    public Aggregation extractRaw(Optional<Long> size) throws NullPointerException {
        requireNonNull(size);

        Aggregation raw = new Aggregation();
        raw.setType(Aggregation.Type.RAW);
        size.ifPresent(sizeValue -> raw.setSize(sizeValue.intValue()));
        return raw;
    }

    private Map<String, String> getFields(List<Expression> columns) {
        Map<String, String> fields = new HashMap<>();
        for (Expression column : columns) {
            fields.put(column.toFormatlessString(), getAlias(column));
        }
        return fields;
    }

    private Map<String, Object> getGroupAttributes(Set<Expression> selectFields, Set<Expression> groupByFields) {
        List<Expression> functions = new ArrayList<>();
        for (Expression selectField : selectFields) {
            if (selectField instanceof FunctionCall) {
                functions.add(selectField);
            } else {
                if (!groupByFields.contains(selectField)) {
                    throw new ParsingException("Select field " + selectField.toFormatlessString()  + " should be a grouping function or in GROUP BY clause");
                }
            }
        }
        Collections.sort(functions, Expression::compareTo);
        Map<String, Object> attributes = new HashMap<>();
        List<Map<String, Object>> operations = getGroupOperations(functions);
        if (!operations.isEmpty()) {
            attributes.put(OPERATIONS, operations);
        }
        return attributes;
    }

    private List<Map<String, Object>> getGroupOperations(List<Expression> functions) {
        List<Map<String, Object>> operations = new ArrayList<>();
        for (Expression selectField : functions) {
            Map<String, Object> operation = new HashMap<>();
            GroupOperationType type = ((FunctionCall) selectField).getType();
            operation.put(OPERATION_TYPE, type);
            if (aliases.containsKey(selectField)) {
                operation.put(NEW_NAME_FIELD, getAlias(selectField));
            }
            if (type != COUNT) {
                operation.put(OPERATION_FIELD, getOperationField((FunctionCall) selectField));
            }
            operations.add(operation);
        }
        return operations;
    }

    private String getOperationField(FunctionCall functionCall) {
        Expression field = functionCall.getArguments().get(0);
        return field.toFormatlessString();
    }

    private String getAlias(Expression expression) {
        if (aliases.containsKey(expression)) {
            return aliases.get(expression).toFormatlessString();
        }

        return expression.toFormatlessString();
    }
}
