/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.util;

import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.tree.QueryNode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExpressionFormatterTest {
    private BQLParser bqlParser = new BQLParser();

    @Test
    public void testSimple() {
        QueryNode queryNode = bqlParser.createQueryNode("select * from stream()");
        Assert.assertEquals(ExpressionFormatter.format(queryNode, true), "SELECT * FROM STREAM()");
    }

    @Test
    public void testEverything() {
        QueryNode queryNode = bqlParser.createQueryNode("select distinct a, b as c from stream(max, time) where d group by e having f order by g asc, h desc windowing every(1, time, first, 1, time) limit 1");
        Assert.assertEquals(ExpressionFormatter.format(queryNode, true), "SELECT DISTINCT a, b AS c FROM STREAM(max, TIME) WHERE d GROUP BY e HAVING f ORDER BY g ASC, h DESC WINDOWING EVERY(1, TIME, FIRST, 1, TIME) LIMIT 1");
    }

    @Test
    public void testTimeOnlyStream() {
        QueryNode queryNode = bqlParser.createQueryNode("select * from stream(2000, time)");
        Assert.assertEquals(ExpressionFormatter.format(queryNode, true), "SELECT * FROM STREAM(2000, TIME)");
    }

    @Test
    public void testWindowIncludeAll() {
        QueryNode queryNode = bqlParser.createQueryNode("select * from stream() windowing every(1, time, all)");
        Assert.assertEquals(ExpressionFormatter.format(queryNode, true), "SELECT * FROM STREAM() WINDOWING EVERY(1, TIME, ALL)");
    }

    @Test
    public void testTumblingWindow() {
        QueryNode queryNode = bqlParser.createQueryNode("select * from stream() windowing tumbling(1, time)");
        Assert.assertEquals(ExpressionFormatter.format(queryNode, true), "SELECT * FROM STREAM() WINDOWING TUMBLING(1, TIME)");
    }
}
