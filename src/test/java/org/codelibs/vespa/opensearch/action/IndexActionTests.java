package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yahoo.jdisc.http.HttpRequest.Method;

public class IndexActionTests {

    @Test
    public void testIsTarget() {
        IndexAction action = new IndexAction(null);

        assertTrue(action.isTarget(Method.POST, "/index/_doc".split("/")));
        assertTrue(action.isTarget(Method.POST, "/index/_doc/".split("/")));
        assertTrue(action.isTarget(Method.POST, "/index/_create/id".split("/")));

        assertFalse(action.isTarget(Method.POST, "/index/_doc/id".split("/")));
        assertFalse(action.isTarget(Method.POST, "/index/_create".split("/")));
        assertFalse(action.isTarget(Method.POST, "/index/_create/".split("/")));

        assertTrue(action.isTarget(Method.PUT, "/index/_doc/id".split("/")));
        assertTrue(action.isTarget(Method.PUT, "/index/_create/id".split("/")));

        assertFalse(action.isTarget(Method.PUT, "/index/_doc".split("/")));
        assertFalse(action.isTarget(Method.PUT, "/index/_doc/".split("/")));
        assertFalse(action.isTarget(Method.PUT, "/index/_create".split("/")));
        assertFalse(action.isTarget(Method.PUT, "/index/_create/".split("/")));

        assertFalse(action.isTarget(Method.GET, "/index/_doc".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index/_doc/".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index/_create/id".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index/_doc/id".split("/")));
        assertFalse(action.isTarget(Method.GET, "/index/_create/id".split("/")));

        assertFalse(action.isTarget(Method.GET, "/".split("/")));
        assertFalse(action.isTarget(Method.GET, "/xxx".split("/")));

    }
}
