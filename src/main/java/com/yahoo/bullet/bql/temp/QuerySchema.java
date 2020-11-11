package com.yahoo.bullet.bql.temp;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class QuerySchema {

    //private Schema schema;

    //private Map<ExpressionNode, FieldExpression> aggregateMapping = new HashMap<>();
    //private Map<ExpressionNode, Expression> mapping = new HashMap<>();

    private List<Field> fields = new ArrayList<>();


    private List<BulletError> typeErrors = new ArrayList<>();


    private int[] schemaLevels = new int[] { 0 };
    private int[] mappingLevels = new int[] { 0 };


    private List<Schema> schemas = new ArrayList<>();
    private List<Map<ExpressionNode, Expression>> mappings = new ArrayList<>();


    private Set<String> transientFields = new HashSet<>();


    // 1) base layer schema
    // 2) projection layer schema (contains fields?)
    // 3) aggregation layer schema
    // 4) computation layer schema











    private Map<ExpressionNode, Expression> mapping1 = new HashMap<>();
    private Map<ExpressionNode, Expression> mapping2 = new HashMap<>();

    private Schema schema1;
    private Schema schema2;

    private int mappingLevel = 1;

    private Map<ExpressionNode, FieldExpression> aliasMapping = new HashMap<>();
    private Map<ExpressionNode, FieldExpression> fieldMapping = new HashMap<>();
    private Schema baseSchema;


    private Map<ExpressionNode, FieldExpression> currentAliasMapping;
    private Map<ExpressionNode, FieldExpression> currentFieldMapping;
    private Schema currentSchema;
    private Map<ExpressionNode, Expression> currentMapping = new HashMap<>();


    public QuerySchema(Schema schema) {
        schemas.add(schema);
        mappings.add(new HashMap<>());



        baseSchema = schema;

        currentAliasMapping = Collections.emptyMap();
        currentFieldMapping = Collections.emptyMap();
        currentSchema = schema;

    }

    public void put(ExpressionNode node, Expression expression) {
        currentMapping.put(node, expression);
        /*
        if (mappingLevel == 1) {
            mapping1.put(node, expression);
        } else if (mappingLevel == 2) {
            mapping2.put(node, expression);
        }
        */
        //mappings.get(mappings.size() - 1).put(node, expression);
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

    public void addAggregateMapping(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        mapping2.put(node, expression);
    }

    public void addFieldToSchema(String name, Type type) {

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





    public void addProjectionField(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        addFieldMapping(node, expression);
        fields.add(new Field(name, expression));
    }

    public void addProjectionField(String name, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        fields.add(new Field(name, expression));
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

    //public void addTransientProjectionField(String name, ExpressionNode node, Type type) {
    //    addProjectionField(name, node, type);
    //    transientFields.add(name);
    //}

    public void addSchemaField(String name, Type type) {
        schema2.addField(name, type);
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        mapping2.put(new FieldExpressionNode(new IdentifierNode(name, false, null), type, null), expression);
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
            currentFieldMapping.putAll(fieldMapping);
            aliasMapping.clear();
            fieldMapping.clear();
        }
        currentMapping.clear();
    }




    public List<Field> getProjectionFields() {
        return new ArrayList<>();
    }

    public Type getType(String field) {
        /*
        if (schema1 == null) {
            return Type.UNKNOWN;
        }
        return schema1.getType(field);
        */
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

    public void incMappingLevel() {
        if (mappingLevel == 1) {
            mappingLevel = 2;
        }
    }
}
