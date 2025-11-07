package org.codelibs.vespa.opensearch.action;

import java.util.Map;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class ClusterHealthAction extends HttpAction {

    public ClusterHealthAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // GET /_cluster/health
        // GET /_cluster/health/<index>
        if (method == Method.GET && paths.length >= 2 && "_cluster".equals(paths[1])) {
            // Match /_cluster/health or /_cluster/health/<index>
            return paths.length >= 3 && "health".equals(paths[2]);
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final Map<String, Object> result = handler.getVespaClient().getClusterHealth();
        return createResponse(httpRequest, 200, result);
    }

}
