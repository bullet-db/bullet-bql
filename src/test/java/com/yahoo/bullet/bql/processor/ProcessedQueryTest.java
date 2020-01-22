package com.yahoo.bullet.bql.processor;

import org.junit.Assert;
import org.testng.annotations.Test;

public class ProcessedQueryTest {
    @Test
    public void testGetRecordDuration() {
        // coverage
        Assert.assertNull(new ProcessedQuery().getRecordDuration());
    }
}
