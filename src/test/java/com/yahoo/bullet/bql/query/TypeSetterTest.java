/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;

public class TypeSetterTest {
    @Test
    public void testConstructor() {
        // coverage
        new TypeSetter();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported unary operation: \\+")
    public void testGetUnaryTypeNotUnary() {
        // coverage
        UnaryExpression expression = Mockito.mock(UnaryExpression.class);
        Mockito.when(expression.getOperand()).thenReturn(expression);
        Mockito.when(expression.getOp()).thenReturn(Operation.ADD);
        TypeSetter.setUnaryType(expression);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported n-ary operation: \\+")
    public void testGetNAryTypeNotNAry() {
        // coverage
        NAryExpression expression = Mockito.mock(NAryExpression.class);
        Mockito.when(expression.getOperands()).thenReturn(Collections.emptyList());
        Mockito.when(expression.getOp()).thenReturn(Operation.ADD);
        TypeSetter.setNAryType(expression);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported group operation: COUNT")
    public void testGetAggregateType() {
        // coverage
        TypeSetter.setAggregateType(null, GroupOperation.GroupOperationType.COUNT, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported binary operation: NOT")
    public void testGetBinaryTypeNotBinary() {
        // coverage
        BinaryExpression expression = Mockito.mock(BinaryExpression.class);
        Mockito.when(expression.getLeft()).thenReturn(expression);
        Mockito.when(expression.getRight()).thenReturn(expression);
        Mockito.when(expression.getOp()).thenReturn(Operation.NOT);
        TypeSetter.setBinaryType(expression, false);
    }
}
