package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class RootActionTests {

    @Test
    public void testIsTarget() {
        RootAction action = new RootAction(null);
        assertTrue(action.isTarget(Method.GET, "/".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index/_doc".split("/")));
    }
}
