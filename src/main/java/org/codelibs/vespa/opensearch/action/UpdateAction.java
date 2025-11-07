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

public class UpdateAction extends HttpAction {

    public UpdateAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // POST /<index>/_update/<id>
        if (method == Method.POST && paths.length == 4 && !paths[1].startsWith("_") && "_update".equals(paths[2])
                && paths[3].length() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        final String[] paths = path.split("/");
        final String indexName = paths[1];
        final String docId = paths[3];

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

        if (requestBody == null) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Request body is required");
            return createResponse(httpRequest, 400, error);
        }

        try {
            client.partialUpdate(indexName, documentType, docId, requestBody);

            final Map<String, Object> result = new HashMap<>();
            result.put("_index", indexName);
            result.put("_id", docId);
            result.put("_version", 1);
            result.put("result", "updated");
            result.put("_shards", Map.of("total", 1, "successful", 1, "failed", 0));

            return createResponse(httpRequest, 200, result);
        } catch (final VespaClientException e) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return createResponse(httpRequest, 404, error);
        }
    }

}
