package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class BulkActionTests {

    @Test
    public void testIsTarget() {
        BulkAction action = new BulkAction(null);

        // Bulk operations
        assertTrue(action.isTarget(Method.POST, "/_bulk".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_bulk".split("/")));
        assertTrue(action.isTarget(Method.PUT, "/_bulk".split("/")));
        assertTrue(action.isTarget(Method.PUT, "/myindex/_bulk".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.GET, "/_bulk".split("/")));
        assertFalse(action.isTarget(Method.POST, "/myindex/_doc".split("/")));
    }
}
