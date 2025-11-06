package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class ClusterHealthActionTests {

    @Test
    public void testIsTarget() {
        ClusterHealthAction action = new ClusterHealthAction(null);
        assertTrue(action.isTarget(Method.GET, "/_cluster/health".split("/")));
        assertTrue(action.isTarget(Method.GET, "/_cluster/health/myindex".split("/")));
        assertFalse(action.isTarget(Method.GET, "/_cluster/state".split("/")));
        assertFalse(action.isTarget(Method.POST, "/_cluster/health".split("/")));
        assertFalse(action.isTarget(Method.GET, "/".split("/")));
    }
}
