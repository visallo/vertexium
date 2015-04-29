package org.vertexium.cli.commands;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SetAuthsCommandTest {
    @Test
    public void testParseAuths() {
        List<String> args = new ArrayList<>();
        args.add("\"a,b\",'c,d'");
        args.add("e");
        List<String> list = SetAuthsCommand.parseAuths(args);
        assertEquals(3, list.size());
        assertEquals("a,b", list.get(0));
        assertEquals("c,d", list.get(1));
        assertEquals("e", list.get(2));
    }
}
