package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class MappingActionTests {

    @Test
    public void testIsTarget() {
        MappingAction action = new MappingAction(null);

        // GET mapping
        assertTrue(action.isTarget(Method.GET, "/myindex/_mapping".split("/")));

        // PUT mapping
        assertTrue(action.isTarget(Method.PUT, "/myindex/_mapping".split("/")));

        // Should not match
        assertFalse(action.isTarget(Method.POST, "/myindex/_mapping".split("/")));
        assertFalse(action.isTarget(Method.GET, "/myindex/_settings".split("/")));
        assertFalse(action.isTarget(Method.GET, "/myindex".split("/")));
    }
}
