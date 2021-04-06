/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ASTVisitorTest {
    private static class MockASTVisitor extends ASTVisitor<Void, Void> {
    }

    private MockASTVisitor visitor;

    @BeforeMethod
    public void setup() {
        visitor = Mockito.spy(new MockASTVisitor());
    }

    @Test
    public void testVisitQuery() {
        QueryNode query = new QueryNode(null, null, null, null, null, null, null, null, null, null);
        visitor.process(query);
        Mockito.verify(visitor).visitQuery(query, null);
    }

    @Test
    public void testVisitSelect() {
        SelectNode select = new SelectNode(false, null, null);
        visitor.process(select);
        Mockito.verify(visitor).visitSelect(select, null);
    }

    @Test
    public void testVisitSelectItem() {
        SelectItemNode selectItem = new SelectItemNode(false, null, null, null);
        visitor.process(selectItem);
        Mockito.verify(visitor).visitSelectItem(selectItem, null);
    }

    @Test
    public void testVisitGroupBy() {
        GroupByNode groupBy = new GroupByNode(null, null);
        visitor.process(groupBy);
        Mockito.verify(visitor).visitGroupBy(groupBy, null);
    }

    @Test
    public void testVisitOrderBy() {
        OrderByNode orderBy = new OrderByNode(null, null);
        visitor.process(orderBy);
        Mockito.verify(visitor).visitOrderBy(orderBy, null);
    }

    @Test
    public void testVisitSortItem() {
        SortItemNode sortItem = new SortItemNode(null, null, null);
        visitor.process(sortItem);
        Mockito.verify(visitor).visitSortItem(sortItem, null);
    }

    @Test
    public void testVisitWindow() {
        WindowNode window = new WindowNode(null, null, null, null);
        visitor.process(window);
        Mockito.verify(visitor).visitWindow(window, null);
    }

    @Test
    public void testVisitWindowInclude() {
        WindowIncludeNode windowInclude = new WindowIncludeNode(50, Window.Unit.TIME, null);
        visitor.process(windowInclude);
        Mockito.verify(visitor).visitWindowInclude(windowInclude, null);
    }

    @Test
    public void testVisitFieldExpression() {
        FieldExpressionNode fieldExpression = new FieldExpressionNode(null, null, null);
        visitor.process(fieldExpression);
        Mockito.verify(visitor).visitFieldExpression(fieldExpression, null);
    }

    @Test
    public void testVisitSubFieldExpression() {
        SubFieldExpressionNode subFieldExpression = new SubFieldExpressionNode(null, null, null, null, null, null, null);
        visitor.process(subFieldExpression);
        Mockito.verify(visitor).visitSubFieldExpression(subFieldExpression, null);
    }

    @Test
    public void testVisitListExpression() {
        ListExpressionNode listExpression = new ListExpressionNode(null, null);
        visitor.process(listExpression);
        Mockito.verify(visitor).visitListExpression(listExpression, null);
    }

    @Test
    public void testVisitNullPredicate() {
        NullPredicateNode nullPredicate = new NullPredicateNode(null, false, null);
        visitor.process(nullPredicate);
        Mockito.verify(visitor).visitNullPredicate(nullPredicate, null);
    }

    @Test
    public void testVisitUnaryExpression() {
        UnaryExpressionNode unaryExpression = new UnaryExpressionNode(null, null, false, null);
        visitor.process(unaryExpression);
        Mockito.verify(visitor).visitUnaryExpression(unaryExpression, null);
    }

    @Test
    public void testVisitNAryExpression() {
        NAryExpressionNode nAryExpression = new NAryExpressionNode(null, null, null);
        visitor.process(nAryExpression);
        Mockito.verify(visitor).visitNAryExpression(nAryExpression, null);
    }

    @Test
    public void testVisitGroupOperation() {
        GroupOperationNode groupOperation = new GroupOperationNode(GroupOperation.GroupOperationType.AVG, null, null);
        visitor.process(groupOperation);
        Mockito.verify(visitor).visitGroupOperation(groupOperation, null);
    }

    @Test
    public void testVisitCountDistinct() {
        CountDistinctNode countDistinct = new CountDistinctNode(null, null);
        visitor.process(countDistinct);
        Mockito.verify(visitor).visitCountDistinct(countDistinct, null);
    }

    @Test
    public void testVisitDistribution() {
        LinearDistributionNode distribution = new LinearDistributionNode(null, null, 10, null);
        visitor.process(distribution);
        Mockito.verify(visitor).visitDistribution(distribution, null);
    }

    @Test
    public void testVisitTopK() {
        TopKNode topK = new TopKNode(50, null, null, null);
        visitor.process(topK);
        Mockito.verify(visitor).visitTopK(topK, null);
    }

    @Test
    public void testVisitCastExpression() {
        CastExpressionNode castExpression = new CastExpressionNode(null, Type.LONG, null);
        visitor.process(castExpression);
        Mockito.verify(visitor).visitCastExpression(castExpression, null);
    }

    @Test
    public void testVisitBinaryExpression() {
        BinaryExpressionNode binaryExpression = new BinaryExpressionNode(null, null, null, null);
        visitor.process(binaryExpression);
        Mockito.verify(visitor).visitBinaryExpression(binaryExpression, null);
    }

    @Test
    public void testVisitParenthesesExpression() {
        ParenthesesExpressionNode parenthesesExpression = new ParenthesesExpressionNode(null, null);
        visitor.process(parenthesesExpression);
        Mockito.verify(visitor).visitParenthesesExpression(parenthesesExpression, null);
    }

    @Test
    public void testVisitIdentifier() {
        IdentifierNode identifier = new IdentifierNode(null, false, null);
        visitor.process(identifier);
        Mockito.verify(visitor).visitIdentifier(identifier, null);
    }

    @Test
    public void testVisitLiteral() {
        LiteralNode literal = new LiteralNode(null, null);
        visitor.process(literal);
        Mockito.verify(visitor).visitLiteral(literal, null);
    }
}
