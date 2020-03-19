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
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.CastExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;

import java.util.Arrays;

public final class QueryUtil {
    public static IdentifierNode identifier(String name) {
        return new IdentifierNode(name, false);
    }

    public static IdentifierNode quotedIdentifier(String name) {
        return new IdentifierNode(name, true);
    }

    public static FieldExpression field(String field, Type type) {
        return new FieldExpression(field, null, null, null, type);
    }

    public static FieldExpression field(String field, Integer index, Type type) {
        return new FieldExpression(field, index, null, null, type);
    }

    public static FieldExpression field(String field, Integer index, String subKey, Type type) {
        return new FieldExpression(field, index, null, subKey, type);
    }

    public static FieldExpression field(String field, String key, Type type) {
        return new FieldExpression(field, null, key, null, type);
    }

    public static FieldExpression field(String field, String key, String subKey, Type type) {
        return new FieldExpression(field, null, key, subKey, type);
    }

    public static ValueExpression value(Object object) {
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

    public static BinaryExpression binary(Expression left, Expression right, Operation op, BinaryExpression.Modifier modifier, Type type) {
        BinaryExpression expression = new BinaryExpression(left, right, op, modifier);
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
