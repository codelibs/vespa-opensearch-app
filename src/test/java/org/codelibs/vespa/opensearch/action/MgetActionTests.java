package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class MgetActionTests {

    @Test
    public void testIsTarget() {
        MgetAction action = new MgetAction(null);

        // Mget operations
        assertTrue(action.isTarget(Method.GET, "/_mget".split("/")));
        assertTrue(action.isTarget(Method.POST, "/_mget".split("/")));
        assertTrue(action.isTarget(Method.GET, "/myindex/_mget".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_mget".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.PUT, "/_mget".split("/")));
        assertFalse(action.isTarget(Method.GET, "/_search".split("/")));
        assertFalse(action.isTarget(Method.GET, "/myindex".split("/")));
    }
}
