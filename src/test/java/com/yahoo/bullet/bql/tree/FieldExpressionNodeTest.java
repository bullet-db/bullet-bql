package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class FieldExpressionNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), Type.LISTOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("---"), 0, identifier("def"), identifier("ghi"), Type.LISTOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("abc"), 1, identifier("def"), identifier("ghi"), Type.LISTOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("---"), identifier("ghi"), Type.LISTOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("---"), Type.LISTOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), Type.MAPOFMAP, Type.DOUBLE),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), Type.LISTOFMAP, Type.FLOAT));
    }
}
