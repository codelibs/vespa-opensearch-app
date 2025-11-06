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

public class IndicesAction extends HttpAction {

    public IndicesAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // PUT /<index> - Create index
        // DELETE /<index> - Delete index
        // GET /<index> - Get index info
        // HEAD /<index> - Check if index exists
        if (paths.length == 2 && paths[1].length() > 0 && !paths[1].startsWith("_")) {
            return method == Method.PUT || method == Method.DELETE || method == Method.GET || method == Method.HEAD;
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
            switch (method) {
            case PUT:
                return handleCreateIndex(httpRequest, indexName, client);
            case DELETE:
                return handleDeleteIndex(httpRequest, indexName, client);
            case GET:
                return handleGetIndex(httpRequest, indexName, client);
            case HEAD:
                return handleIndexExists(httpRequest, indexName, client);
            default:
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

    private HttpResponse handleCreateIndex(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
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

        @SuppressWarnings("unchecked")
        final Map<String, Object> settings = requestBody != null ? (Map<String, Object>) requestBody.get("settings") : null;
        final Map<String, Object> result = client.createIndex(indexName, settings);

        // If mappings are provided, update them
        if (requestBody != null && requestBody.containsKey("mappings")) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> mappings = (Map<String, Object>) requestBody.get("mappings");
            client.updateMapping(indexName, mappings);
        }

        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleDeleteIndex(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
        final Map<String, Object> result = client.deleteIndex(indexName);
        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleGetIndex(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
        final Map<String, Object> result = client.getIndex(indexName);
        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleIndexExists(final HttpRequest httpRequest, final String indexName, final VespaClient client) {
        final boolean exists = client.indexExists(indexName);
        return new HttpResponse(exists ? 200 : 404) {
            @Override
            public void render(final java.io.OutputStream stream) throws IOException {
                // HEAD request should not have a body
            }
        };
    }

}
