package com.yahoo.bullet.bql.temp;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
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
import java.util.Arrays;
import java.util.HashMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class QuerySchema {
    private static final Map<DistributionType, Schema> DISTRIBUTION_SCHEMAS = new HashMap<>();

    static {
        DISTRIBUTION_SCHEMAS.put(DistributionType.QUANTILE,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.VALUE_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.QUANTILE_FIELD, Type.DOUBLE))));
        DISTRIBUTION_SCHEMAS.put(DistributionType.PMF,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.COUNT_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.RANGE_FIELD, Type.STRING))));
        DISTRIBUTION_SCHEMAS.put(DistributionType.CDF,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.COUNT_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.RANGE_FIELD, Type.STRING))));
    }

    private Set<Field> projectionFields = new LinkedHashSet<>();
    private Set<Field> computationFields = new LinkedHashSet<>();

    private List<BulletError> typeErrors = new ArrayList<>();

    private Map<String, Type> schema = new HashMap<>();
    private Map<String, String> aliasMapping = new HashMap<>();

    private Schema baseSchema;

    private Map<String, Type> currentSchema;
    private Map<String, String> currentAliasMapping = new HashMap<>();
    private Map<ExpressionNode, Expression> currentMapping = new HashMap<>();

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

    // Messy
    public Expression get(ExpressionNode node) {
        if (node == null) {
            return null;
        } else if (node instanceof FieldExpressionNode) {
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

    public boolean hasAliasOrField(ExpressionNode node) {
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

    public void addCurrentProjectionField(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        projectionFields.add(new Field(name, expression));
        currentSchema.put(name, type);
    }

    public void nextLevel(boolean replaceCurrent) {
        if (replaceCurrent) {
            currentSchema = schema;
            currentAliasMapping = aliasMapping;
            aliasMapping = new HashMap<>();
        } else {
            if (currentSchema != null) {
                currentSchema.putAll(schema);
            } else {
                currentSchema = schema;
            }
            currentAliasMapping.putAll(aliasMapping);
            aliasMapping.clear();
        }
        schema = new HashMap<>();
        currentMapping.clear();
    }

    public List<Field> getProjectionFields() {
        return new ArrayList<>(projectionFields);
    }

    public List<Field> getComputationFields() {
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

    public void setDistributionFields(DistributionType type) {
        schema = DISTRIBUTION_SCHEMAS.get(type);
    }
}
