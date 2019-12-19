/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class ExpressionTest {
    /*private ExpressionImpl expression;

    @BeforeClass
    public void setUp() {
        NodeLocation location = new NodeLocation(1, 1);
        expression = new ExpressionImpl(location);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "\\QNot yet implemented\\E.*")
    public void testAccept() {
        DefaultTraversalVisitor visitor = new DefaultTraversalVisitor() {
        };
        DefaultTraversalVisitor spy = Mockito.spy(visitor);
        visitor.process(expression);

        verify(spy).visitExpression(expression, null);
    }

    @Test
    public void testGetType() {
        assertEquals(expression.getType(SelectItemNode.Type.class), SelectItemNode.Type.NON_SELECT);
    }

    @Test
    public void testCompareTo() {
        assertEquals(noLocationExpression().compareTo(noLocationExpression()), 0);
        assertEquals(expression.compareTo(noLocationExpression()), 1);
        assertEquals(noLocationExpression().compareTo(expression), -1);

        NodeLocation diffLine = new NodeLocation(2, 1);
        assertEquals(expression.compareTo(new ExpressionImpl(diffLine)), -1);

        NodeLocation diffColumn = new NodeLocation(1, 2);
        assertEquals(expression.compareTo(new ExpressionImpl(diffColumn)), -1);
    }

    private class ExpressionImpl extends ExpressionNode {
        public ExpressionImpl(NodeLocation location) {
            this(Optional.of(location));
        }

        public ExpressionImpl(Optional<NodeLocation> location) {
            super(location);
        }

        @Override
        public List<? extends Node> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }

    private ExpressionNode noLocationExpression() {
        return new ExpressionImpl(Optional.empty());
    }*/
}
