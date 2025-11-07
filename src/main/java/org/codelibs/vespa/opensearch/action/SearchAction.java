package org.codelibs.vespa.opensearch.action;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class SearchAction extends HttpAction {

    public SearchAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // GET /<index>/_search
        // POST /<index>/_search
        // GET /_search
        // POST /_search
        if ((method == Method.GET || method == Method.POST)) {
            if (paths.length == 2 && "_search".equals(paths[1])) {
                return true; // /_search
            }
            if (paths.length == 3 && !paths[1].startsWith("_") && "_search".equals(paths[2])) {
                return true; // /<index>/_search
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
        final String documentType = handler.getDocumentType();

        Map<String, Object> requestBody = null;
        try (InputStream is = httpRequest.getData()) {
            if (is.available() > 0) {
                requestBody = JsonXContent.jsonXContent
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, is).map();
            }
        } catch (final IOException e) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Failed to parse request body: " + e.getMessage());
            return createResponse(httpRequest, 400, error);
        }

        // If no request body, create empty search (match_all)
        if (requestBody == null) {
            requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        }

        try {
            final Map<String, Object> result = client.search(indexName != null ? indexName : "default", documentType, requestBody);
            return createResponse(httpRequest, 200, result);
        } catch (final VespaClientException e) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return createResponse(httpRequest, 500, error);
        }
    }

}
