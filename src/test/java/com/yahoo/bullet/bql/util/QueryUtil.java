/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/QueryUtil.java
 */
package com.yahoo.bullet.bql.util;

import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;

import java.io.Serializable;
import java.util.Arrays;

public final class QueryUtil {
    public static IdentifierNode identifier(String name) {
        return new IdentifierNode(name, false, null);
    }

    public static IdentifierNode quotedIdentifier(String name) {
        return new IdentifierNode(name, true, null);
    }

    public static FieldExpression field(String field, Type type) {
        FieldExpression expression = new FieldExpression(field);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, Integer index, Type type) {
        FieldExpression expression = new FieldExpression(field, index);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, Integer index, String subKey, Type type) {
        FieldExpression expression = new FieldExpression(field, index, subKey);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, Integer index, Expression variableSubKey, Type type) {
        FieldExpression expression = new FieldExpression(field, index, variableSubKey);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, String key, Type type) {
        FieldExpression expression = new FieldExpression(field, key);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, Expression variableKey, Type type) {
        FieldExpression expression = new FieldExpression(field, variableKey);
        expression.setType(type);
        return expression;
    }

    public static FieldExpression field(String field, String key, String subKey, Type type) {
        FieldExpression expression = new FieldExpression(field, key, subKey);
        expression.setType(type);
        return expression;
    }

    public static ValueExpression value(Serializable object) {
        return new ValueExpression(object);
    }

    public static UnaryExpression unary(Expression operand, Operation op, Type type) {
        UnaryExpression expression = new UnaryExpression(operand, op);
        expression.setType(type);
        return expression;
    }

    public static BinaryExpression binary(Expression left, Expression right, Operation op, Type type) {
        BinaryExpression expression = new BinaryExpression(left, right, op);
        expression.setType(type);
        return expression;
    }

    public static NAryExpression nary(Type type, Operation op, Expression... expressions) {
        NAryExpression expression = new NAryExpression(Arrays.asList(expressions), op);
        expression.setType(type);
        return expression;
    }

    public static CastExpression cast(Expression value, Type castType, Type type) {
        CastExpression expression = new CastExpression(value, castType);
        expression.setType(type);
        return expression;
    }

    public static ListExpression list(Type type, Expression... expressions) {
        ListExpression expression = new ListExpression(Arrays.asList(expressions));
        expression.setType(type);
        return expression;
    }
}
