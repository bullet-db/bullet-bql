/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
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
        QueryNode query = new QueryNode(new SelectNode(false, Collections.emptyList()),
                                        new StreamNode(null, null),
                                        new LiteralNode(true),
                                        new GroupByNode(Collections.emptyList()),
                                        new LiteralNode(false),
                                        new OrderByNode(Collections.emptyList()),
                                        new WindowNode(null, null, null),
                                        null);
        visitor.process(query);
        Mockito.verify(visitor).visitQuery(query, null);
        Mockito.verify(visitor).visitSelect(query.getSelect(), null);
        Mockito.verify(visitor).visitStream(query.getStream(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) query.getWhere(), null);
        Mockito.verify(visitor).visitGroupBy(query.getGroupBy(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) query.getWhere(), null);
        Mockito.verify(visitor).visitOrderBy(query.getOrderBy(), null);
        Mockito.verify(visitor).visitWindow(query.getWindow(), null);

    }

    @Test
    public void testVisitSelect() {
        SelectNode select = new SelectNode(false, Arrays.asList(new SelectItemNode(false, null, null),
                                                                new SelectItemNode(true, null, null)));
        visitor.process(select);
        Mockito.verify(visitor).visitSelect(select, null);
        Mockito.verify(visitor).visitSelectItem(select.getSelectItems().get(0), null);
        Mockito.verify(visitor).visitSelectItem(select.getSelectItems().get(1), null);
    }

    @Test
    public void testVisitSelectItem() {
        SelectItemNode selectItem = new SelectItemNode(false, new LiteralNode(5), null);
        visitor.process(selectItem);
        Mockito.verify(visitor).visitSelectItem(selectItem, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) selectItem.getExpression(), null);
    }

    @Test
    public void testVisitGroupBy() {
        GroupByNode groupBy = new GroupByNode(Arrays.asList(new LiteralNode(5),
                                                            new LiteralNode(6)));
        visitor.process(groupBy);
        Mockito.verify(visitor).visitGroupBy(groupBy, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupBy.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupBy.getExpressions().get(1), null);
    }

    @Test
    public void testVisitOrderBy() {
        OrderByNode orderBy = new OrderByNode(Arrays.asList(new SortItemNode(null, SortItemNode.Ordering.ASCENDING),
                                                            new SortItemNode(null, SortItemNode.Ordering.DESCENDING)));
        visitor.process(orderBy);
        Mockito.verify(visitor).visitOrderBy(orderBy, null);
        Mockito.verify(visitor).visitSortItem(orderBy.getSortItems().get(0), null);
        Mockito.verify(visitor).visitSortItem(orderBy.getSortItems().get(1), null);
    }

    @Test
    public void testVisitSortItem() {
        SortItemNode sortItem = new SortItemNode(new LiteralNode(5), null);
        visitor.process(sortItem);
        Mockito.verify(visitor).visitSortItem(sortItem, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) sortItem.getExpression(), null);
    }

    @Test
    public void testVisitWindow() {
        WindowNode window = new WindowNode(null, null, new WindowIncludeNode("50", "TIME"));
        visitor.process(window);
        Mockito.verify(visitor).visitWindow(window, null);
        Mockito.verify(visitor).visitWindowInclude(window.getWindowInclude(), null);
    }

    @Test
    public void testVisitListExpression() {
        ListExpressionNode listExpression = new ListExpressionNode(Arrays.asList(new LiteralNode(5),
                                                                                 new LiteralNode(6)));
        visitor.process(listExpression);
        Mockito.verify(visitor).visitListExpression(listExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) listExpression.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) listExpression.getExpressions().get(1), null);
    }

    @Test
    public void testVisitNullPredicate() {
        NullPredicateNode nullPredicate = new NullPredicateNode(new LiteralNode(5), false);
        visitor.process(nullPredicate);
        Mockito.verify(visitor).visitNullPredicate(nullPredicate, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nullPredicate.getExpression(), null);
    }

    @Test
    public void testVisitUnaryExpression() {
        UnaryExpressionNode unaryExpression = new UnaryExpressionNode(null, new LiteralNode(5), false);
        visitor.process(unaryExpression);
        Mockito.verify(visitor).visitUnaryExpression(unaryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) unaryExpression.getExpression(), null);
    }

    @Test
    public void testVisitNAryExpression() {
        NAryExpressionNode nAryExpression = new NAryExpressionNode(null, Arrays.asList(new LiteralNode(5),
                                                                                       new LiteralNode(6)));
        visitor.process(nAryExpression);
        Mockito.verify(visitor).visitNAryExpression(nAryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nAryExpression.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) nAryExpression.getExpressions().get(1), null);
    }

    @Test
    public void testVisitGroupOperation() {
        GroupOperationNode groupOperation = new GroupOperationNode("AVG", new LiteralNode(5));
        visitor.process(groupOperation);
        Mockito.verify(visitor).visitGroupOperation(groupOperation, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) groupOperation.getExpression(), null);
    }

    @Test
    public void testVisitCountDistinct() {
        CountDistinctNode countDistinct = new CountDistinctNode(Arrays.asList(new LiteralNode(5),
                                                                              new LiteralNode(6)));
        visitor.process(countDistinct);
        Mockito.verify(visitor).visitCountDistinct(countDistinct, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) countDistinct.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) countDistinct.getExpressions().get(1), null);
    }

    @Test
    public void testVisitDistribution() {
        LinearDistributionNode distribution = new LinearDistributionNode(null, new LiteralNode(5), null);
        visitor.process(distribution);
        Mockito.verify(visitor).visitDistribution(distribution, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) distribution.getExpression(), null);
    }

    @Test
    public void testVisitTopK() {
        TopKNode topK = new TopKNode("50", "50", Arrays.asList(new LiteralNode(5),
                                                               new LiteralNode(6)));
        visitor.process(topK);
        Mockito.verify(visitor).visitTopK(topK, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) topK.getExpressions().get(0), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) topK.getExpressions().get(1), null);
    }

    @Test
    public void testVisitCastExpression() {
        CastExpressionNode castExpression = new CastExpressionNode(new LiteralNode(5), "LONG");
        visitor.process(castExpression);
        Mockito.verify(visitor).visitCastExpression(castExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) castExpression.getExpression(), null);
    }

    @Test
    public void testVisitBinaryExpression() {
        BinaryExpressionNode binaryExpression = new BinaryExpressionNode(new LiteralNode(5), new LiteralNode(6), Operation.ADD);
        visitor.process(binaryExpression);
        Mockito.verify(visitor).visitBinaryExpression(binaryExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) binaryExpression.getLeft(), null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) binaryExpression.getRight(), null);
    }

    @Test
    public void testVisitParenthesesExpression() {
        ParenthesesExpressionNode parenthesesExpression = new ParenthesesExpressionNode(new LiteralNode(5));
        visitor.process(parenthesesExpression);
        Mockito.verify(visitor).visitParenthesesExpression(parenthesesExpression, null);
        Mockito.verify(visitor).visitLiteral((LiteralNode) parenthesesExpression.getExpression(), null);
    }
}
