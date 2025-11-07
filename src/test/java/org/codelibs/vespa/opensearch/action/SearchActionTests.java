package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class SearchActionTests {

    @Test
    public void testIsTarget() {
        SearchAction action = new SearchAction(null);

        // Search operations
        assertTrue(action.isTarget(Method.GET, "/_search".split("/")));
        assertTrue(action.isTarget(Method.POST, "/_search".split("/")));
        assertTrue(action.isTarget(Method.GET, "/myindex/_search".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_search".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.PUT, "/_search".split("/")));
        assertFalse(action.isTarget(Method.GET, "/_count".split("/")));
        assertFalse(action.isTarget(Method.GET, "/myindex".split("/")));
    }
}
