package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window;
import org.testng.annotations.Test;

public class WindowNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new WindowNode(5000L, Window.Unit.TIME, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(2000L, Window.Unit.TIME, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(5000L, Window.Unit.RECORD, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(5000L, Window.Unit.TIME, new WindowIncludeNode(500L, Window.Unit.TIME)));
    }
}
