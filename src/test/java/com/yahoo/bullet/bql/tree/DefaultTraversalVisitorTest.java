/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class DefaultTraversalVisitorTest {
    private static class MockDefaultTraversalVisitor extends DefaultTraversalVisitor<Void, Void> {
    }

    private MockDefaultTraversalVisitor visitor;

    @BeforeMethod
    public void setup() {
        visitor = Mockito.spy(new MockDefaultTraversalVisitor());
    }

    @Test
    public void testVisitQuery() {
        QueryNode query = new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                        new StreamNode(null, null),
                                        new LateralViewNode(null, null),
                                        new LiteralNode(true, null),
                                        new GroupByNode(Collections.emptyList(), null),
                                        new LiteralNode(false, null),
                                        new OrderByNode(Collections.emptyList(), null),
                                        new WindowNode(null, null, null, null),
                                        null,
                                        null);
        visitor.process(query);
        Mockito.verify(visitor).visitQuery(query, null);
        Mockito.verify(visitor).visitSelect(query.getSelect(), null);
        Mockito.verify(visitor).visitStream(query.getStream(), null);
        Mockito.verify(visitor).visitLateralView(query.getLateralView(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) query.getWhere(), null);
        Mockito.verify(visitor).visitGroupBy(query.getGroupBy(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) query.getWhere(), null);
        Mockito.verify(visitor).visitOrderBy(query.getOrderBy(), null);
        Mockito.verify(visitor).visitWindow(query.getWindow(), null);

    }

    @Test
    public void testVisitSelect() {
        SelectNode select = new SelectNode(false, Arrays.asList(new SelectItemNode(false, null, null, null),
                                                                new SelectItemNode(true, null, null, null)), null);
        visitor.process(select);
        Mockito.verify(visitor).visitSelect(select, null);
        Mockito.verify(visitor).visitSelectItem(select.getSelectItems().get(0), null);
        Mockito.verify(visitor).visitSelectItem(select.getSelectItems().get(1), null);
    }

    @Test
    public void testVisitSelectItem() {
        SelectItemNode selectItem = new SelectItemNode(false, new LiteralNode(5, null), null, null);
        visitor.process(selectItem);
        Mockito.verify(visitor).visitSelectItem(selectItem, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) selectItem.getExpression(), null);
    }

    @Test
    public void testVisitGroupBy() {
        GroupByNode groupBy = new GroupByNode(Arrays.asList(new LiteralNode(5, null),
                                                            new LiteralNode(6, null)), null);
        visitor.process(groupBy);
        Mockito.verify(visitor).visitGroupBy(groupBy, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupBy.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupBy.getExpressions().get(1), null);
    }

    @Test
    public void testVisitOrderBy() {
        OrderByNode orderBy = new OrderByNode(Arrays.asList(new SortItemNode(null, SortItemNode.Ordering.ASCENDING, null),
                                                            new SortItemNode(null, SortItemNode.Ordering.DESCENDING, null)), null);
        visitor.process(orderBy);
        Mockito.verify(visitor).visitOrderBy(orderBy, null);
        Mockito.verify(visitor).visitSortItem(orderBy.getSortItems().get(0), null);
        Mockito.verify(visitor).visitSortItem(orderBy.getSortItems().get(1), null);
    }

    @Test
    public void testVisitSortItem() {
        SortItemNode sortItem = new SortItemNode(new LiteralNode(5, null), null, null);
        visitor.process(sortItem);
        Mockito.verify(visitor).visitSortItem(sortItem, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) sortItem.getExpression(), null);
    }

    @Test
    public void testVisitWindow() {
        WindowNode window = new WindowNode(null, null, new WindowIncludeNode(50, Window.Unit.TIME, null), null);
        visitor.process(window);
        Mockito.verify(visitor).visitWindow(window, null);
        Mockito.verify(visitor).visitWindowInclude(window.getWindowInclude(), null);
    }

    @Test
    public void testVisitListExpression() {
        ListExpressionNode listExpression = new ListExpressionNode(Arrays.asList(new LiteralNode(5, null),
                                                                                 new LiteralNode(6, null)), null);
        visitor.process(listExpression);
        Mockito.verify(visitor).visitListExpression(listExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) listExpression.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) listExpression.getExpressions().get(1), null);
    }

    @Test
    public void testVisitNullPredicate() {
        NullPredicateNode nullPredicate = new NullPredicateNode(new LiteralNode(5, null), false, null);
        visitor.process(nullPredicate);
        Mockito.verify(visitor).visitNullPredicate(nullPredicate, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nullPredicate.getExpression(), null);
    }

    @Test
    public void testVisitUnaryExpression() {
        UnaryExpressionNode unaryExpression = new UnaryExpressionNode(null, new LiteralNode(5, null), false, null);
        visitor.process(unaryExpression);
        Mockito.verify(visitor).visitUnaryExpression(unaryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) unaryExpression.getExpression(), null);
    }

    @Test
    public void testVisitNAryExpression() {
        NAryExpressionNode nAryExpression = new NAryExpressionNode(null, Arrays.asList(new LiteralNode(5, null),
                                                                                       new LiteralNode(6, null)), null);
        visitor.process(nAryExpression);
        Mockito.verify(visitor).visitNAryExpression(nAryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nAryExpression.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nAryExpression.getExpressions().get(1), null);
    }

    @Test
    public void testVisitGroupOperation() {
        GroupOperationNode groupOperation = new GroupOperationNode(GroupOperation.GroupOperationType.AVG, new LiteralNode(5, null), null);
        visitor.process(groupOperation);
        Mockito.verify(visitor).visitGroupOperation(groupOperation, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupOperation.getExpression(), null);
    }

    @Test
    public void testVisitCountDistinct() {
        CountDistinctNode countDistinct = new CountDistinctNode(Arrays.asList(new LiteralNode(5, null),
                                                                              new LiteralNode(6, null)), null);
        visitor.process(countDistinct);
        Mockito.verify(visitor).visitCountDistinct(countDistinct, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) countDistinct.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) countDistinct.getExpressions().get(1), null);
    }

    @Test
    public void testVisitDistribution() {
        LinearDistributionNode distribution = new LinearDistributionNode(null, new LiteralNode(5, null), 10, null);
        visitor.process(distribution);
        Mockito.verify(visitor).visitDistribution(distribution, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) distribution.getExpression(), null);
    }

    @Test
    public void testVisitTopK() {
        TopKNode topK = new TopKNode(50, 50L, Arrays.asList(new LiteralNode(5, null),
                                                            new LiteralNode(6, null)), null);
        visitor.process(topK);
        Mockito.verify(visitor).visitTopK(topK, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) topK.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) topK.getExpressions().get(1), null);
    }

    @Test
    public void testVisitCastExpression() {
        CastExpressionNode castExpression = new CastExpressionNode(new LiteralNode(5, null), Type.LONG, null);
        visitor.process(castExpression);
        Mockito.verify(visitor).visitCastExpression(castExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) castExpression.getExpression(), null);
    }

    @Test
    public void testVisitBinaryExpression() {
        BinaryExpressionNode binaryExpression = new BinaryExpressionNode(new LiteralNode(5, null), new LiteralNode(6, null), Operation.ADD, null);
        visitor.process(binaryExpression);
        Mockito.verify(visitor).visitBinaryExpression(binaryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) binaryExpression.getLeft(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) binaryExpression.getRight(), null);
    }

    @Test
    public void testVisitParenthesesExpression() {
        ParenthesesExpressionNode parenthesesExpression = new ParenthesesExpressionNode(new LiteralNode(5, null), null);
        visitor.process(parenthesesExpression);
        Mockito.verify(visitor).visitParenthesesExpression(parenthesesExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) parenthesesExpression.getExpression(), null);
    }
}
