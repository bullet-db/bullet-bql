/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;

public class DefaultTraversalVisitorTest {
    private TraversalTestVisitor visitor;

    private class TraversalTestVisitor extends DefaultTraversalVisitor<Void, Void> { }

    @BeforeClass
    public void setUp() {
        visitor = new TraversalTestVisitor();
    }

    /*@Test
    public void testVisitQuery() {
        With with = simpleWith();
        QuerySpecification querySpecification = simpleQuerySpecification(selectList(identifier("aaa")));
        OrderByNode orderBy = simpleOrderBy();
        Query query = new Query(Optional.of(with), querySpecification, Optional.of(orderBy), Optional.empty());

        TraversalTestVisitor spy = spy(visitor);
        spy.process(query);
        verify(spy).visitQuery(query, null);
        verify(spy).visitWith(with, null);
        verify(spy).visitQuerySpecification(querySpecification, null);
        verify(spy).visitOrderBy(orderBy, null);
        verify(spy).visitNode(with, null);
    }

    @Test
    public void testVisitQueryNoWithNoOrderBy() {
        QuerySpecification querySpecification = simpleQuerySpecification(selectList(identifier("aaa")));
        Query query = new Query(Optional.empty(), querySpecification, Optional.empty(), Optional.empty());

        TraversalTestVisitor spy = spy(visitor);
        spy.process(query);
        verify(spy).visitQuery(query, null);
        verify(spy, never()).visitWith(any(With.class), any(Void.class));
        verify(spy).visitQuerySpecification(querySpecification, null);
        verify(spy, never()).visitOrderBy(any(OrderByNode.class), any(Void.class));
    }

    @Test
    public void testVisitFunctionCall() {
        ExpressionNode filter = equal(identifier("aaa"), identifier("bbb"));
        OrderByNode orderBy = simpleOrderBy();
        ExpressionNode argument = identifier("ccc");

        FunctionCall functionCall = new FunctionCall(
                COUNT,
                Optional.of(filter),
                Optional.of(orderBy),
                true,
                singletonList(argument));

        TraversalTestVisitor spy = spy(visitor);
        spy.process(functionCall);
        verify(spy).visitFunctionCall(functionCall, null);
        verify(spy).process(filter, null);
        verify(spy).process(argument, null);
        verify(spy).visitOrderBy(orderBy, null);
    }

    @Test
    public void testVisitFunctionCallNoFilterNoOrderBy() {
        FunctionCall functionCall = simpleFunctionCall();

        TraversalTestVisitor spy = spy(visitor);
        spy.process(functionCall);
        verify(spy, times(2)).process(any(ExpressionNode.class), any(Void.class));
        verify(spy).visitFunctionCall(functionCall, null);
        verify(spy, never()).visitOrderBy(any(OrderByNode.class), any(Void.class));
    }

    @Test
    public void testVisitArithmeticUnary() {
        DecimalLiteralNode value = new DecimalLiteralNode("10.5");
        ArithmeticUnaryExpression arithmeticUnaryExpression = new ArithmeticUnaryExpression(PLUS, value);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(arithmeticUnaryExpression);
        verify(spy).visitArithmeticUnary(arithmeticUnaryExpression, null);
        verify(spy).visitDecimalLiteral(value, null);
    }

    @Test
    public void testVisitNotExpression() {
        ExpressionNode value = equal(identifier("aaa"), identifier("bbb"));
        NotExpression notExpression = new NotExpression(value);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(notExpression);
        verify(spy).visitNotExpression(notExpression, null);
        verify(spy).process(value, null);
    }

    @Test
    public void testVisitLikePredicateWithEscape() {
        ExpressionNode value = identifier("aaa");
        ListExpressionNode patterns = new ListExpressionNode(singletonList(identifier("bbb")));
        ExpressionNode escape = identifier("``");
        LikePredicate likePredicate = new LikePredicate(value, patterns, Optional.of(escape));

        TraversalTestVisitor spy = spy(visitor);
        spy.process(likePredicate);
        verify(spy).visitLikePredicate(likePredicate, null);
        verify(spy).process(escape, null);
    }

    @Test
    public void testVisitLogicalBinaryExpression() {
        ExpressionNode left = equal(identifier("aaa"), identifier("bbb"));
        ExpressionNode right = equal(identifier("ccc"), identifier("ddd"));
        LogicalBinaryExpression logicalBinaryExpression = new LogicalBinaryExpression(AND, left, right);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(logicalBinaryExpression);
        verify(spy).process(left, null);
        verify(spy).process(right, null);
    }

    @Test
    public void testVisitQuerySpecification() {
        SelectNode select = selectList(identifier("aaa"));
        StreamNode stream = new StreamNode(Optional.of("10"), Optional.of("20"));
        ExpressionNode where = equal(identifier("bbb"), identifier("ccc"));
        SimpleGroupBy simpleGroupBy = new SimpleGroupBy(singletonList(identifier("ddd")));
        GroupByNode groupBy = new GroupByNode(true, singletonList(simpleGroupBy));
        ExpressionNode having = equal(identifier("eee"), identifier("fff"));
        OrderByNode orderBy = simpleOrderBy();
        WindowIncludeNode include = simpleWindowInclude();
        WindowNode windowing = new WindowNode((long) 100, TIME, include);
        QuerySpecification querySpecification = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));

        TraversalTestVisitor spy = spy(visitor);
        spy.process(querySpecification);
        verify(spy).visitQuerySpecification(querySpecification, null);
        verify(spy).visitStream(stream, null);
        verify(spy).process(where, null);
        verify(spy).visitGroupBy(groupBy, null);
        verify(spy).visitSimpleGroupBy(simpleGroupBy, null);
        verify(spy).process(having, null);
        verify(spy).visitOrderBy(orderBy, null);
        verify(spy).visitWindow(windowing, null);
        verify(spy).visitWindowInclude(include, null);
    }*/
}
