/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/test/java/com/facebook/presto/sql/parser/TestSqlParser.java
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.bql.tree.QueryNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BQLParserTest {
    private BQLParser parser = new BQLParser();

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*Identifiers must not start with a digit; surround the identifier with double quotes\\.")
    public void testDigitIdentifier() {
        parser.createQueryNode("SELECT 0abc FROM STREAM()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*Identifiers must not be empty strings\\.")
    public void testQuotedIdentifier() {
        parser.createQueryNode("SELECT \"\" FROM STREAM()");
    }

    @Test
    public void testNonReserved() {
        QueryNode node = parser.createQueryNode("SELECT all FROM STREAM()");
        Assert.assertEquals(node.getSelect().getSelectItems().get(0).getExpression().getName(), "all");
    }
}
