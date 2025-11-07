package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class UpdateActionTests {

    @Test
    public void testIsTarget() {
        UpdateAction action = new UpdateAction(null);

        // Update operations
        assertTrue(action.isTarget(Method.POST, "/myindex/_update/123".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.PUT, "/myindex/_update/123".split("/")));
        assertFalse(action.isTarget(Method.POST, "/myindex/_doc/123".split("/")));
        assertFalse(action.isTarget(Method.POST, "/myindex/_update".split("/")));
        assertFalse(action.isTarget(Method.POST, "/_update/123".split("/")));
    }
}
