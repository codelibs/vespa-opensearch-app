package org.codelibs.vespa.opensearch.action;

import java.util.Map;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class ClusterStateAction extends HttpAction {

    public ClusterStateAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // GET /_cluster/state
        return method == Method.GET && paths.length >= 3 && "_cluster".equals(paths[1]) && "state".equals(paths[2]);
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final Map<String, Object> result = handler.getVespaClient().getClusterState();
        return createResponse(httpRequest, 200, result);
    }

}
