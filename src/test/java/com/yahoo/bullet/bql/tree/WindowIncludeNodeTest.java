package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window;
import org.testng.annotations.Test;

public class WindowIncludeNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new WindowIncludeNode(50L, Window.Unit.RECORD),
                              new WindowIncludeNode(500L, Window.Unit.RECORD),
                              new WindowIncludeNode(50L, Window.Unit.TIME));
    }
}
