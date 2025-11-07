package org.codelibs.vespa.opensearch.action;

import java.util.HashMap;
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class RefreshAction extends HttpAction {

    public RefreshAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // POST /<index>/_refresh
        // POST /_refresh
        if (method == Method.POST) {
            if (paths.length == 2 && "_refresh".equals(paths[1])) {
                return true; // /_refresh
            }
            if (paths.length == 3 && !paths[1].startsWith("_") && "_refresh".equals(paths[2])) {
                return true; // /<index>/_refresh
            }
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        final String[] paths = path.split("/");
        final String indexName = paths.length == 3 ? paths[1] : null;

        final VespaClient client = handler.getVespaClient();

        final Map<String, Object> result = client.refresh(indexName != null ? indexName : "_all");
        return createResponse(httpRequest, 200, result);
    }

}
