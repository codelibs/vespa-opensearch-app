package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class IndicesActionTests {

    @Test
    public void testIsTarget() {
        IndicesAction action = new IndicesAction(null);

        // Create index
        assertTrue(action.isTarget(Method.PUT, "/myindex".split("/")));

        // Delete index
        assertTrue(action.isTarget(Method.DELETE, "/myindex".split("/")));

        // Get index
        assertTrue(action.isTarget(Method.GET, "/myindex".split("/")));

        // Check index exists
        assertTrue(action.isTarget(Method.HEAD, "/myindex".split("/")));

        // Should not match special paths
        assertFalse(action.isTarget(Method.PUT, "/_cluster/health".split("/")));
        assertFalse(action.isTarget(Method.GET, "/".split("/")));
    }
}
