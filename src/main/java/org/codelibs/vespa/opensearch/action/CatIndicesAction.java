package org.codelibs.vespa.opensearch.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class CatIndicesAction extends HttpAction {

    public CatIndicesAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // GET /_cat/indices
        // GET /_cat/indices/<index>
        return method == Method.GET && paths.length >= 2 && "_cat".equals(paths[1])
                && (paths.length == 3 && "indices".equals(paths[2]) || paths.length == 4);
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final Map<String, Map<String, Object>> indices = handler.getVespaClient().getAllIndices();

        final List<Map<String, Object>> result = new ArrayList<>();
        for (final Map.Entry<String, Map<String, Object>> entry : indices.entrySet()) {
            final Map<String, Object> index = new HashMap<>();
            index.put("health", "green");
            index.put("status", "open");
            index.put("index", entry.getKey());
            index.put("uuid", entry.getValue().get("uuid"));
            index.put("pri", "1");
            index.put("rep", "0");
            index.put("docs.count", "0");
            index.put("docs.deleted", "0");
            index.put("store.size", "0b");
            index.put("pri.store.size", "0b");
            result.add(index);
        }

        return createResponse(httpRequest, 200, Map.of("indices", result));
    }

}
