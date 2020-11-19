package com.yahoo.bullet.bql.temp;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class QuerySchema {
    private static final Map<DistributionType, Map<ExpressionNode, FieldExpression>> DISTRIBUTION_FIELDS = new HashMap<>();

    static {
        Map<ExpressionNode, FieldExpression> quantileFields = new HashMap<>();
        quantileFields.put(fieldNode(QuantileSketch.VALUE_FIELD, Type.DOUBLE), field(QuantileSketch.VALUE_FIELD, Type.DOUBLE));
        quantileFields.put(fieldNode(QuantileSketch.QUANTILE_FIELD, Type.DOUBLE), field(QuantileSketch.QUANTILE_FIELD, Type.DOUBLE));

        Map<ExpressionNode, FieldExpression> pmfFields = new HashMap<>();
        pmfFields.put(fieldNode(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE), field(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE));
        pmfFields.put(fieldNode(QuantileSketch.COUNT_FIELD, Type.DOUBLE), field(QuantileSketch.COUNT_FIELD, Type.DOUBLE));
        pmfFields.put(fieldNode(QuantileSketch.RANGE_FIELD, Type.STRING), field(QuantileSketch.RANGE_FIELD, Type.STRING));

        DISTRIBUTION_FIELDS.put(DistributionType.QUANTILE, quantileFields);
        DISTRIBUTION_FIELDS.put(DistributionType.PMF, pmfFields);
        DISTRIBUTION_FIELDS.put(DistributionType.CDF, pmfFields);
    }

    private Set<Field> fields = new LinkedHashSet<>();
    private Set<Field> computationFields = new LinkedHashSet<>();


    private List<BulletError> typeErrors = new ArrayList<>();

    private Set<String> transientFields = new HashSet<>();


    // 1) base layer schema
    // 2) projection layer schema (contains fields?)
    // 3) aggregation layer schema
    // 4) computation layer schema


    private Map<ExpressionNode, FieldExpression> aliasMapping = new HashMap<>();
    private Map<ExpressionNode, FieldExpression> fieldMapping = new HashMap<>();
    private Schema baseSchema;


    private Map<ExpressionNode, FieldExpression> currentAliasMapping;
    private Map<ExpressionNode, FieldExpression> currentFieldMapping;
    private Schema currentSchema;
    private Map<ExpressionNode, Expression> currentMapping = new HashMap<>();

    public QuerySchema(Schema schema) {
        baseSchema = schema;
        currentAliasMapping = new HashMap<>();
        currentFieldMapping = new HashMap<>();
        currentSchema = schema;
    }

    public void put(ExpressionNode node, Expression expression) {
        currentMapping.put(node, expression);
    }

    public Expression get(ExpressionNode node) {
        Expression expression;
        if ((expression = currentAliasMapping.get(node)) != null) {
            return expression;
        }
        if ((expression = currentFieldMapping.get(node)) != null) {
            return expression;
        }
        return currentMapping.get(node);
    }

    public Expression getAliasOrField(ExpressionNode node) {
        Expression expression;
        if ((expression = currentAliasMapping.get(node)) != null) {
            return expression;
        }
        return currentFieldMapping.get(node);
    }

    public void addAggregateMapping(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
    }

    public void addAliasMapping(String name, Type type) {
        FieldExpressionNode expressionNode = new FieldExpressionNode(new IdentifierNode(name, false, null), type, null);
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        aliasMapping.put(expressionNode, expression);
    }

    public void addFieldMapping(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        fieldMapping.put(node, expression);
    }

    public void addFieldMapping(ExpressionNode node, FieldExpression expression) {
        fieldMapping.put(node, expression);
    }

    public void addProjectionField(String name, ExpressionNode node, Expression expression) {
        FieldExpression fieldExpression = new FieldExpression(name);
        fieldExpression.setType(expression.getType());
        addFieldMapping(node, fieldExpression);
        fields.add(new Field(name, expression));
    }

    public void addProjectionField(String name, Expression expression) {
        fields.add(new Field(name, expression));
    }

    public void addComputationField(String name, ExpressionNode node, Expression expression) {
        FieldExpression fieldExpression = new FieldExpression(name);
        fieldExpression.setType(expression.getType());
        addFieldMapping(node, fieldExpression);
        computationFields.add(new Field(name, expression));
    }


    public void addCurrentProjectionField(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        currentFieldMapping.put(node, expression);
        fields.add(new Field(name, expression));
    }

    public void addNonAliasedProjectionField(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        fields.add(new Field(name, expression));
    }

    public void nextLevel(boolean replaceCurrent) {
        if (replaceCurrent) {
            currentAliasMapping = aliasMapping;
            currentFieldMapping = fieldMapping;
            currentSchema = new Schema();
            aliasMapping = new HashMap<>();
            fieldMapping = new HashMap<>();
        } else {
            currentAliasMapping.putAll(aliasMapping);
            //currentFieldMapping.putAll(fieldMapping);
            // extremely inefficient
            fieldMapping.forEach((expressionNode, fieldExpression) -> {
                currentFieldMapping.entrySet().stream()
                                              .filter(entry -> fieldExpression.getField().equals(entry.getValue().getField()))
                                              .forEach(entry -> entry.setValue(new FieldExpression(fieldExpression.getField())));
                currentFieldMapping.put(expressionNode, fieldExpression);
            });




            aliasMapping.clear();
            fieldMapping.clear();
        }
        currentMapping.clear();
    }

    public List<Field> getProjectionFields() {
        return new ArrayList<>(fields);
    }

    public List<Field> getComputationFields() {
        return new ArrayList<>(computationFields);
    }

    public Type getType(String field) {
        if (currentSchema == null) {
            return Type.UNKNOWN;
        }
        return currentSchema.getType(field);
    }

    public Type getBaseSchemaType(String field) {
        if (baseSchema == null) {
            return Type.UNKNOWN;
        }
        return baseSchema.getType(field);
    }

    public void addTypeError(BulletError error) {
        typeErrors.add(error);
    }

    public void addTypeError(ExpressionNode node, String message) {
        typeErrors.add(new BulletError(node.getLocation() + message, (List<String>) null));
    }

    public void addTypeErrors(List<BulletError> errors) {
        typeErrors.addAll(errors);
    }

    public static FieldExpressionNode fieldNode(String name, Type type) {
        return new FieldExpressionNode(new IdentifierNode(name, false, null), type, null);
    }

    public static FieldExpression field(String name, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        return expression;
    }

    public void addDistributionFields(DistributionType type) {
        aliasMapping.putAll(DISTRIBUTION_FIELDS.get(type));
    }
}
