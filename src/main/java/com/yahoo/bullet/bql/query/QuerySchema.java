/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QuerySchema {
    private static final Map<DistributionType, Map<String, Type>> DISTRIBUTION_FIELDS = new HashMap<>();

    static {
        Map<String, Type> quantileFields = new HashMap<>();
        quantileFields.put(QuantileSketch.VALUE_FIELD, Type.DOUBLE);
        quantileFields.put(QuantileSketch.QUANTILE_FIELD, Type.DOUBLE);

        Map<String, Type> pmfFields = new HashMap<>();
        pmfFields.put(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE);
        pmfFields.put(QuantileSketch.COUNT_FIELD, Type.DOUBLE);
        pmfFields.put(QuantileSketch.RANGE_FIELD, Type.STRING);

        DISTRIBUTION_FIELDS.put(DistributionType.QUANTILE, quantileFields);
        DISTRIBUTION_FIELDS.put(DistributionType.PMF, pmfFields);
        DISTRIBUTION_FIELDS.put(DistributionType.CDF, pmfFields);
    }

    @Getter
    private Map<String, Type> currentSchema;
    @Getter
    private Map<String, String> currentAliasMapping = new HashMap<>();
    private Map<ExpressionNode, Expression> currentMapping = new HashMap<>();

    private Map<String, Type> schema = new HashMap<>();
    private Map<String, String> aliasMapping = new HashMap<>();

    private Schema baseSchema;

    private Set<Field> projectionFields = new LinkedHashSet<>();
    private Set<Field> computationFields = new LinkedHashSet<>();

    @Getter
    private List<BulletError> errors = new ArrayList<>();

    public QuerySchema(Schema schema) {
        baseSchema = schema;
        if (schema != null) {
            currentSchema = new HashMap<>();
            schema.getFields().forEach(field -> currentSchema.put(field.getName(), field.getType()));
        }
    }

    public void put(ExpressionNode node, Expression expression) {
        currentMapping.put(node, expression);
    }

    public Expression get(ExpressionNode node) {
        if (node instanceof FieldExpressionNode) {
            Type type = ((FieldExpressionNode) node).getType();
            if (type != null) {
                return field(node.getName(), type);
            }
        } else if (node instanceof SubFieldExpressionNode) {
            Type type = ((SubFieldExpressionNode) node).getType();
            if (type != null) {
                return field(node.getName(), type);
            }
        }
        if (currentSchema == null) {
            return null;
        }
        String name = node.getName();
        Type type = currentSchema.get(name);
        if (type != null) {
            return field(name, type);
        }
        String alias = currentAliasMapping.get(name);
        if (alias != null) {
            return field(alias, currentSchema.get(alias));
        }
        return currentMapping.get(node);
    }

    public boolean contains(ExpressionNode node) {
        if (currentSchema == null) {
            return false;
        }
        String name = node.getName();
        return currentSchema.containsKey(name) || currentAliasMapping.containsKey(name);
    }

    public void addProjectionField(String name, Expression expression) {
        projectionFields.add(new Field(name, expression));
    }

    public void addComputationField(String name, Expression expression) {
        computationFields.add(new Field(name, expression));
    }

    public void addSchemaField(String name, Type type) {
        schema.put(name, type);
    }

    public void addCurrentSchemaField(String name, Type type) {
        currentSchema.put(name, type);
    }

    public void addAlias(String name, String alias) {
        aliasMapping.put(name, alias);
    }

    public void setDistributionFieldsSchema(DistributionType type) {
        schema = new HashMap<>(DISTRIBUTION_FIELDS.get(type));
    }

    public void nextLayer(boolean replaceCurrent) {
        if (replaceCurrent) {
            currentSchema = schema;
            currentAliasMapping = aliasMapping;
            schema = new HashMap<>();
            aliasMapping = new HashMap<>();
        } else if (currentSchema != null) {
            currentSchema.putAll(schema);
            currentAliasMapping.putAll(aliasMapping);
            schema.clear();
            aliasMapping.clear();
        } else {
            currentSchema = schema;
            currentAliasMapping.putAll(aliasMapping);
            schema = new HashMap<>();
            aliasMapping.clear();
        }
        currentMapping.clear();
    }

    /**
     * Checks the list of projection fields for duplicates before returning them.
     *
     * @return
     */
    public List<Field> getProjectionFields() {
        Map<String, Long> fieldsToCount = projectionFields.stream().map(Field::getName).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> duplicates = fieldsToCount.entrySet().stream().filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
        if (!duplicates.isEmpty()) {
            errors.add(new BulletError("The following field names are shared: " + duplicates, "Please specify non-overlapping field names."));
        }
        return new ArrayList<>(projectionFields);
    }

    /**
     *
     * @return
     */
    public List<Field> getComputationFields() {
        Map<String, Long> fieldsToCount = computationFields.stream().map(Field::getName).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> duplicates = fieldsToCount.entrySet().stream().filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
        if (!duplicates.isEmpty()) {
            errors.add(new BulletError("The following field names/aliases are shared: " + duplicates, "Please specify non-overlapping field names and aliases."));
        }
        return new ArrayList<>(computationFields);
    }

    public Type getType(String field) {
        if (currentSchema == null) {
            return Type.UNKNOWN;
        }
        return currentSchema.get(field);
    }

    public Type getBaseSchemaType(String field) {
        if (baseSchema == null) {
            return Type.UNKNOWN;
        }
        return baseSchema.getType(field);
    }

    public void addError(ExpressionNode node, String message) {
        errors.add(new BulletError(node.getLocation() + message, (List<String>) null));
    }

    public void addErrors(List<BulletError> bulletErrors) {
        errors.addAll(bulletErrors);
    }

    private static FieldExpression field(String name, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        return expression;
    }
}
