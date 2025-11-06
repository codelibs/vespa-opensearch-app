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

public class MappingAction extends HttpAction {

    public MappingAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // GET /<index>/_mapping
        // PUT /<index>/_mapping
        if (paths.length >= 3 && !paths[1].startsWith("_") && "_mapping".equals(paths[2])) {
            return method == Method.GET || method == Method.PUT;
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        final String[] paths = path.split("/");
        final String indexName = paths.length > 1 ? paths[1] : null;

        if (indexName == null || indexName.isEmpty()) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Index name is required");
            return createResponse(httpRequest, 400, error);
        }

        final VespaClient client = handler.getVespaClient();
        final Method method = httpRequest.getMethod();

        try {
            if (method == Method.GET) {
                return handleGetMapping(httpRequest, indexName, client);
            } else if (method == Method.PUT) {
                return handleUpdateMapping(httpRequest, indexName, client);
            } else {
                final Map<String, Object> error = new HashMap<>();
                error.put("error", "Unsupported method: " + method);
                return createResponse(httpRequest, 405, error);
            }
        } catch (final VespaClientException e) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return createResponse(httpRequest, 404, error);
        }
    }

    private HttpResponse handleGetMapping(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
        final Map<String, Object> result = client.getMapping(indexName);
        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleUpdateMapping(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
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

        if (requestBody == null) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Request body is required");
            return createResponse(httpRequest, 400, error);
        }

        final Map<String, Object> result = client.updateMapping(indexName, requestBody);
        return createResponse(httpRequest, 200, result);
    }

}
