package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class RefreshActionTests {

    @Test
    public void testIsTarget() {
        RefreshAction action = new RefreshAction(null);

        // Refresh operations
        assertTrue(action.isTarget(Method.POST, "/_refresh".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_refresh".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.GET, "/_refresh".split("/")));
        assertFalse(action.isTarget(Method.POST, "/_search".split("/")));
        assertFalse(action.isTarget(Method.PUT, "/myindex/_refresh".split("/")));
    }
}
