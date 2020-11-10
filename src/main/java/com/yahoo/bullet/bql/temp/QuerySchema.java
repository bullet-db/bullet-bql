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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class QuerySchema {
    public enum LayerType {

    }

    public static class Layer {

    }

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





    public QuerySchema(Schema schema) {
        schemas.add(schema);
        mappings.add(new HashMap<>());
    }

    public void put(ExpressionNode node, Expression expression) {
        if (mappingLevel == 1) {
            mapping1.put(node, expression);
        } else if (mappingLevel == 2) {
            mapping2.put(node, expression);
        }
        //mappings.get(mappings.size() - 1).put(node, expression);
    }

    public Expression get(ExpressionNode node) {
        if (mappingLevel == 1) {
            return mapping1.get(node);
        } else if (mappingLevel == 2) {
            return mapping2.get(node);
        } else {
            return null;
        }
        //return mappings.get(mappingLevels[0]).get(node);
    }

    public void addAggregateMapping(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        mapping2.put(node, expression);
    }

    public void addFieldToSchema(String name, Type type) {

    }

    public void addProjectionField(String name, ExpressionNode node, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        mapping2.putIfAbsent(node, expression);


        fields.add(new Field(name, expression));
    }

    public void addTransientProjectionField(String name, ExpressionNode node, Type type) {
        addProjectionField(name, node, type);
        transientFields.add(name);
    }

    public void addSchemaField(String name, Type type) {
        schema2.addField(name, type);
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        mapping2.put(new FieldExpressionNode(new IdentifierNode(name, false, null), type, null), expression);
    }




    public List<Field> getProjectionFields() {
        return new ArrayList<>();
    }

    public Type getType(String field) {
        if (schema1 == null) {
            return Type.UNKNOWN;
        }
        return schema1.getType(field);
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
