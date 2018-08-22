/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign.PLUS;
import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleFunctionCall;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleOrderBy;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuerySpecification;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWindowInclude;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWith;
import static com.yahoo.bullet.parsing.Clause.Operation.AND;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DefaultTraversalVisitorTest {
    private TraversalTestVisitor visitor;

    private class TraversalTestVisitor extends DefaultTraversalVisitor<Void, Void> { }

    @BeforeClass
    public void setUp() {
        visitor = new TraversalTestVisitor();
    }

    @Test
    public void testVisitQuery() {
        With with = simpleWith();
        QuerySpecification querySpecification = simpleQuerySpecification(selectList(identifier("aaa")));
        OrderBy orderBy = simpleOrderBy();
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
        verify(spy, never()).visitOrderBy(any(OrderBy.class), any(Void.class));
    }

    @Test
    public void testVisitFunctionCall() {
        Expression filter = equal(identifier("aaa"), identifier("bbb"));
        OrderBy orderBy = simpleOrderBy();
        Expression argument = identifier("ccc");

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
        verify(spy, times(2)).process(any(Expression.class), any(Void.class));
        verify(spy).visitFunctionCall(functionCall, null);
        verify(spy, never()).visitOrderBy(any(OrderBy.class), any(Void.class));
    }

    @Test
    public void testVisitArithmeticUnary() {
        DecimalLiteral value = new DecimalLiteral("10.5");
        ArithmeticUnaryExpression arithmeticUnaryExpression = new ArithmeticUnaryExpression(PLUS, value);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(arithmeticUnaryExpression);
        verify(spy).visitArithmeticUnary(arithmeticUnaryExpression, null);
        verify(spy).visitDecimalLiteral(value, null);
    }

    @Test
    public void testVisitNotExpression() {
        Expression value = equal(identifier("aaa"), identifier("bbb"));
        NotExpression notExpression = new NotExpression(value);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(notExpression);
        verify(spy).visitNotExpression(notExpression, null);
        verify(spy).process(value, null);
    }

    @Test
    public void testVisitLikePredicateWithEscape() {
        Expression value = identifier("aaa");
        ValueListExpression patterns = new ValueListExpression(singletonList(identifier("bbb")));
        Expression escape = identifier("``");
        LikePredicate likePredicate = new LikePredicate(value, patterns, Optional.of(escape));

        TraversalTestVisitor spy = spy(visitor);
        spy.process(likePredicate);
        verify(spy).visitLikePredicate(likePredicate, null);
        verify(spy).process(escape, null);
    }

    @Test
    public void testVisitLogicalBinaryExpression() {
        Expression left = equal(identifier("aaa"), identifier("bbb"));
        Expression right = equal(identifier("ccc"), identifier("ddd"));
        LogicalBinaryExpression logicalBinaryExpression = new LogicalBinaryExpression(AND, left, right);

        TraversalTestVisitor spy = spy(visitor);
        spy.process(logicalBinaryExpression);
        verify(spy).process(left, null);
        verify(spy).process(right, null);
    }

    @Test
    public void testVisitQuerySpecification() {
        Select select = selectList(identifier("aaa"));
        Stream stream = new Stream(Optional.of("10"), Optional.of("20"));
        Expression where = equal(identifier("bbb"), identifier("ccc"));
        SimpleGroupBy simpleGroupBy = new SimpleGroupBy(singletonList(identifier("ddd")));
        GroupBy groupBy = new GroupBy(true, singletonList(simpleGroupBy));
        Expression having = equal(identifier("eee"), identifier("fff"));
        OrderBy orderBy = simpleOrderBy();
        WindowInclude include = simpleWindowInclude();
        Windowing windowing = new Windowing((long) 100, TIME, include);
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
        verify(spy).visitWindowing(windowing, null);
        verify(spy).visitWindowInclude(include, null);
    }
}
