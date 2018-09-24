/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign.PLUS;
import static com.yahoo.bullet.bql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.yahoo.bullet.bql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.yahoo.bullet.bql.tree.LogicalBinaryExpression.and;
import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.linearQuantile;
import static com.yahoo.bullet.bql.util.QueryUtil.logicalNot;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleBetween;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleFunctionCall;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleInPredicate;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleLikePredicate;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuery;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuerySpecification;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleSortItem;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleTopK;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleValueList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWindowing;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWithQuery;
import static com.yahoo.bullet.bql.util.QueryUtil.unaliasedName;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ASTVisitorTest {
    private ASTTestVisitor visitor;

    private class ASTTestVisitor extends ASTVisitor<Void, Void> { }

    @BeforeClass
    public void setUp() {
        visitor = new ASTTestVisitor();
    }

    @Test
    public void testVisitQuery() {
        BetweenPredicate betweenPredicate = simpleBetween();

        ASTTestVisitor spy = spy(visitor);
        spy.process(betweenPredicate);
        verify(spy).visitBetweenPredicate(betweenPredicate, null);
        verify(spy).visitExpression(betweenPredicate, null);
    }

    @Test
    public void testVisitStatement() {
        Statement query = simpleQuery(selectList(identifier("aaa")));

        ASTTestVisitor spy = spy(visitor);
        spy.process(query);
        verify(spy).visitStatement(query, null);
        verify(spy).visitQuery((Query) query, null);
    }

    @Test
    public void testVisitComparisonExpression() {
        ComparisonExpression comparison = (ComparisonExpression) equal(identifier("aaa"), identifier("bbb"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(comparison);
        verify(spy).visitComparisonExpression(comparison, null);
        verify(spy).visitExpression(comparison, null);
    }

    @Test
    public void testVisitWithQuery() {
        WithQuery withQuery = simpleWithQuery();

        ASTTestVisitor spy = spy(visitor);
        spy.process(withQuery);
        verify(spy).visitWithQuery(withQuery, null);
        verify(spy).visitNode(withQuery, null);
    }

    @Test
    public void testVisitSelect() {
        Select select = selectList(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(select);
        verify(spy).visitSelect(select, null);
        verify(spy).visitNode(select, null);
    }

    @Test
    public void testVisitQuerySpecification() {
        QuerySpecification querySpecification = simpleQuerySpecification(selectList(identifier("aaa")));

        ASTTestVisitor spy = spy(visitor);
        spy.process(querySpecification);
        verify(spy).visitQuerySpecification(querySpecification, null);
        verify(spy).visitQueryBody(querySpecification, null);
    }

    @Test
    public void testVisitInPredicate() {
        InPredicate inPredicate = simpleInPredicate();

        ASTTestVisitor spy = spy(visitor);
        spy.process(inPredicate);
        verify(spy).visitInPredicate(inPredicate, null);
        verify(spy).visitExpression(inPredicate, null);
    }

    @Test
    public void testVisitValueListExpression() {
        ValueListExpression valueListExpression = simpleValueList();

        ASTTestVisitor spy = spy(visitor);
        spy.process(valueListExpression);
        verify(spy).visitValueListExpression(valueListExpression, null);
        verify(spy).visitExpression(valueListExpression, null);
    }

    @Test
    public void testVisitLikePredicate() {
        LikePredicate likePredicate = simpleLikePredicate();

        ASTTestVisitor spy = spy(visitor);
        spy.process(likePredicate);
        verify(spy).visitLikePredicate(likePredicate, null);
        verify(spy).visitExpression(likePredicate, null);
    }

    @Test
    public void testVisitFunctionCall() {
        FunctionCall functionCall = simpleFunctionCall();

        ASTTestVisitor spy = spy(visitor);
        spy.process(functionCall);
        verify(spy).visitFunctionCall(functionCall, null);
        verify(spy).visitExpression(functionCall, null);
    }

    @Test
    public void testVisitBooleanLiteral() {
        BooleanLiteral booleanLiteral = new BooleanLiteral("TRUE");

        ASTTestVisitor spy = spy(visitor);
        spy.process(TRUE_LITERAL);
        spy.process(FALSE_LITERAL);
        spy.process(booleanLiteral);
        verify(spy, times(2)).visitBooleanLiteral(TRUE_LITERAL, null);
        verify(spy, times(2)).visitLiteral(TRUE_LITERAL, null);
        verify(spy).visitBooleanLiteral(FALSE_LITERAL, null);
        verify(spy).visitLiteral(FALSE_LITERAL, null);
    }

    @Test
    public void testVisitDistribution() {
        Distribution linearDistribution = linearQuantile((long) 10);

        ASTTestVisitor spy = spy(visitor);
        spy.process(linearDistribution);
        verify(spy).visitDistribution(linearDistribution, null);
        verify(spy).visitExpression(linearDistribution, null);
    }

    @Test
    public void testVisitTopK() {
        TopK topK = simpleTopK((long) 10);

        ASTTestVisitor spy = spy(visitor);
        spy.process(topK);
        verify(spy).visitTopK(topK, null);
        verify(spy).visitExpression(topK, null);
    }

    @Test
    public void testVisitDereferenceExpression() {
        DereferenceExpression dereference = new DereferenceExpression(identifier("aaa"), identifier("bbb"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(dereference);
        verify(spy).visitDereferenceExpression(dereference, null);
        verify(spy).visitExpression(dereference, null);
    }

    @Test
    public void testVisitNullLiteral() {
        NullLiteral nullLiteral = new NullLiteral();

        ASTTestVisitor spy = spy(visitor);
        spy.process(nullLiteral);
        verify(spy).visitNullLiteral(nullLiteral, null);
        verify(spy).visitLiteral(nullLiteral, null);
    }

    @Test
    public void testVisitArithmeticUnary() {
        ArithmeticUnaryExpression arithmeticUnary = new ArithmeticUnaryExpression(PLUS, new DoubleLiteral("5.5"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(arithmeticUnary);
        verify(spy).visitArithmeticUnary(arithmeticUnary, null);
        verify(spy).visitExpression(arithmeticUnary, null);
    }

    @Test
    public void testVisitNotExpression() {
        NotExpression notExpression = (NotExpression) logicalNot(equal(identifier("aaa"), identifier("bbb")));

        ASTTestVisitor spy = spy(visitor);
        spy.process(notExpression);
        verify(spy).visitNotExpression(notExpression, null);
        verify(spy).visitExpression(notExpression, null);
    }

    @Test
    public void testVisitSingleColumn() {
        SingleColumn singleColumn = (SingleColumn) unaliasedName("aaa");

        ASTTestVisitor spy = spy(visitor);
        spy.process(singleColumn);
        verify(spy).visitSingleColumn(singleColumn, null);
        verify(spy).visitSelectItem(singleColumn, null);
    }

    @Test
    public void testVisitIsNotNullPredicate() {
        IsNotNullPredicate isNotNullPredicate = new IsNotNullPredicate(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(isNotNullPredicate);
        verify(spy).visitIsNotNullPredicate(isNotNullPredicate, null);
        verify(spy).visitExpression(isNotNullPredicate, null);
    }

    @Test
    public void testVisitIsNullPredicate() {
        IsNullPredicate isNullPredicate = new IsNullPredicate(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(isNullPredicate);
        verify(spy).visitIsNullPredicate(isNullPredicate, null);
        verify(spy).visitExpression(isNullPredicate, null);
    }

    @Test
    public void testVisitIsNotEmptyPredicate() {
        IsNotEmptyPredicate isNotEmptyPredicate = new IsNotEmptyPredicate(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(isNotEmptyPredicate);
        verify(spy).visitIsNotEmptyPredicate(isNotEmptyPredicate, null);
        verify(spy).visitExpression(isNotEmptyPredicate, null);
    }

    @Test
    public void testVisitIsEmptyPredicate() {
        IsEmptyPredicate isEmptyPredicate = new IsEmptyPredicate(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(isEmptyPredicate);
        verify(spy).visitIsEmptyPredicate(isEmptyPredicate, null);
        verify(spy).visitExpression(isEmptyPredicate, null);
    }

    @Test
    public void testVisitLogicalBinaryExpression() {
        LogicalBinaryExpression logicalBinary = (LogicalBinaryExpression) and(
                new IsNullPredicate(identifier("aaa")), new IsNotNullPredicate(identifier("bbb")));

        ASTTestVisitor spy = spy(visitor);
        spy.process(logicalBinary);
        verify(spy).visitLogicalBinaryExpression(logicalBinary, null);
        verify(spy).visitExpression(logicalBinary, null);
    }

    @Test
    public void testVisitSortItem() {
        SortItem sortItem = simpleSortItem();

        ASTTestVisitor spy = spy(visitor);
        spy.process(sortItem);
        verify(spy).visitSortItem(sortItem, null);
        verify(spy).visitNode(sortItem, null);
    }

    @Test
    public void testVisitWindowing() {
        Windowing windowing = simpleWindowing();

        ASTTestVisitor spy = spy(visitor);
        spy.process(windowing);
        verify(spy).visitWindowing(windowing, null);
        verify(spy).visitNode(windowing, null);
    }

    @Test
    public void testVisitGroupBy() {
        SimpleGroupBy simpleGroupBy = new SimpleGroupBy(singletonList(identifier("aaa")));
        GroupBy groupBy = new GroupBy(true, singletonList(simpleGroupBy));

        ASTTestVisitor spy = spy(visitor);
        spy.process(groupBy);
        verify(spy).visitGroupBy(groupBy, null);
        verify(spy).visitNode(groupBy, null);
    }

    @Test
    public void testVisitSimpleGroupByTest() {
        SimpleGroupBy simpleGroupBy = new SimpleGroupBy(singletonList(identifier("aaa")));

        ASTTestVisitor spy = spy(visitor);
        spy.process(simpleGroupBy);
        verify(spy).visitSimpleGroupBy(simpleGroupBy, null);
        verify(spy).visitGroupingElement(simpleGroupBy, null);
        verify(spy).visitNode(simpleGroupBy, null);
    }

    @Test
    public void testVisitCastExpression() {
        CastExpression castExpression = new CastExpression(identifier("aaa"), "FLOAT");

        ASTTestVisitor spy = spy(visitor);
        spy.process(castExpression);
        verify(spy).visitCastExpression(castExpression, null);
        verify(spy).visitNode(castExpression, null);
    }

    @Test
    public void testVisitBinaryExpression() {
        BinaryExpression binaryExpression = new BinaryExpression(identifier("aaa"), new DoubleLiteral("5.0"), "+");

        ASTTestVisitor spy = spy(visitor);
        spy.process(binaryExpression);
        verify(spy).visitBinaryExpression(binaryExpression, null);
        verify(spy).visitNode(binaryExpression, null);
    }

    @Test
    public void testVisitParensExpression() {
        ParensExpression parensExpression = new ParensExpression(identifier("aaa"));

        ASTTestVisitor spy = spy(visitor);
        spy.process(parensExpression);
        verify(spy).visitParensExpression(parensExpression, null);
        verify(spy).visitNode(parensExpression, null);
    }
}
