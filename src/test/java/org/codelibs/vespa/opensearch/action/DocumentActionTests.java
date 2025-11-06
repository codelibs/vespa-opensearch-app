package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class DocumentActionTests {

    @Test
    public void testIsTarget() {
        DocumentAction action = new DocumentAction(null);

        // POST operations
        assertTrue(action.isTarget(Method.POST, "/myindex/_doc".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_doc/123".split("/")));
        assertTrue(action.isTarget(Method.POST, "/myindex/_create/123".split("/")));

        // PUT operations
        assertTrue(action.isTarget(Method.PUT, "/myindex/_doc/123".split("/")));
        assertTrue(action.isTarget(Method.PUT, "/myindex/_create/123".split("/")));

        // GET operations
        assertTrue(action.isTarget(Method.GET, "/myindex/_doc/123".split("/")));

        // DELETE operations
        assertTrue(action.isTarget(Method.DELETE, "/myindex/_doc/123".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.GET, "/myindex".split("/")));
        assertFalse(action.isTarget(Method.POST, "/_bulk".split("/")));
        assertFalse(action.isTarget(Method.PUT, "/myindex".split("/")));
    }
}
