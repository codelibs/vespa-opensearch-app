package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class CountActionTests {

    @Test
    public void testIsTarget() {
        CountAction action = new CountAction(null);

        // Count operations
        assertTrue(action.isTarget(Method.GET, "/_count".split("/")));
        assertTrue(action.isTarget(Method.POST, "/_count".split("/")));
        assertTrue(action.isTarget(Method.GET, "/myindex/_count".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_count".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.PUT, "/_count".split("/")));
        assertFalse(action.isTarget(Method.GET, "/_search".split("/")));
        assertFalse(action.isTarget(Method.GET, "/myindex".split("/")));
    }
}
