package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ParensExpressionTest {
    private Expression value;
    private String castType;
    private ParensExpression parensExpression;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        parensExpression = new ParensExpression(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(value);
        assertEquals(parensExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        ParensExpression copy = parensExpression;
        assertTrue(parensExpression.equals(copy));
        assertFalse(parensExpression.equals(null));
        assertFalse(parensExpression.equals(value));

        ParensExpression parensExpressionDiffValue = new ParensExpression(identifier("bbb"));
        assertFalse(parensExpression.equals(parensExpressionDiffValue));
    }

    @Test
    public void testHashCode() {
        ParensExpression sameParensExpression = new ParensExpression(identifier("aaa"));
        assertEquals(parensExpression.hashCode(), sameParensExpression.hashCode());
    }
}
