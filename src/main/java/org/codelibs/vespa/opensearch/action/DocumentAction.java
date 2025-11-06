package org.codelibs.vespa.opensearch.action;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class DocumentAction extends HttpAction {

    public DocumentAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // POST /<index>/_doc - Auto-generate ID
        // POST /<index>/_doc/<id> - Index with ID
        // PUT /<index>/_doc/<id> - Update/create with ID
        // GET /<index>/_doc/<id> - Get document
        // DELETE /<index>/_doc/<id> - Delete document
        // POST /<index>/_create/<id> - Create (fail if exists)
        // PUT /<index>/_create/<id> - Create (fail if exists)
        if (paths.length >= 3 && !paths[1].startsWith("_")) {
            final String action = paths[2];
            if ("_doc".equals(action) || "_create".equals(action)) {
                switch (method) {
                case POST:
                    return paths.length == 3 || paths.length == 4;
                case PUT:
                    return paths.length == 4 && paths[3].length() > 0;
                case GET:
                case DELETE:
                    return paths.length == 4 && paths[3].length() > 0;
                default:
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        final String[] paths = path.split("/");
        final String indexName = paths.length > 1 ? paths[1] : null;
        final String action = paths.length > 2 ? paths[2] : null;
        final String docId = paths.length > 3 && paths[3].length() > 0 ? paths[3] : null;

        if (indexName == null || indexName.isEmpty()) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Index name is required");
            return createResponse(httpRequest, 400, error);
        }

        final VespaClient client = handler.getVespaClient();
        final String documentType = handler.getDocumentType();
        final Method method = httpRequest.getMethod();

        try {
            switch (method) {
            case POST:
                return handlePost(httpRequest, indexName, documentType, docId, action, client);
            case PUT:
                return handlePut(httpRequest, indexName, documentType, docId, action, client);
            case GET:
                return handleGet(httpRequest, indexName, documentType, docId, client);
            case DELETE:
                return handleDelete(httpRequest, indexName, documentType, docId, client);
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

    private HttpResponse handlePost(final HttpRequest httpRequest, final String indexName, final String documentType,
            final String docId, final String action, final VespaClient client) {
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

        final String id = docId != null ? docId : UUID.randomUUID().toString();
        final Map<String, Object> vespaResult = client.insert(indexName, documentType, id, requestBody);

        final Map<String, Object> result = new HashMap<>();
        result.put("_index", indexName);
        result.put("_id", id);
        result.put("_version", 1);
        result.put("result", docId == null ? "created" : "updated");
        result.put("_shards", Map.of("total", 1, "successful", 1, "failed", 0));

        return createResponse(httpRequest, 201, result);
    }

    private HttpResponse handlePut(final HttpRequest httpRequest, final String indexName, final String documentType,
            final String docId, final String action, final VespaClient client) {
        if (docId == null || docId.isEmpty()) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Document ID is required for PUT");
            return createResponse(httpRequest, 400, error);
        }

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

        final Map<String, Object> vespaResult = client.update(indexName, documentType, docId, requestBody);

        final Map<String, Object> result = new HashMap<>();
        result.put("_index", indexName);
        result.put("_id", docId);
        result.put("_version", 1);
        result.put("result", "updated");
        result.put("_shards", Map.of("total", 1, "successful", 1, "failed", 0));

        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleGet(final HttpRequest httpRequest, final String indexName, final String documentType,
            final String docId, final VespaClient client) {
        if (docId == null || docId.isEmpty()) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Document ID is required for GET");
            return createResponse(httpRequest, 400, error);
        }

        final Map<String, Object> vespaResult = client.get(indexName, documentType, docId);

        final Map<String, Object> result = new HashMap<>();
        result.put("_index", indexName);
        result.put("_id", docId);
        result.put("_version", 1);
        result.put("found", true);

        // Extract fields from Vespa response
        if (vespaResult.containsKey("fields")) {
            result.put("_source", vespaResult.get("fields"));
        } else {
            result.put("_source", new HashMap<>());
        }

        return createResponse(httpRequest, 200, result);
    }

    private HttpResponse handleDelete(final HttpRequest httpRequest, final String indexName, final String documentType,
            final String docId, final VespaClient client) {
        if (docId == null || docId.isEmpty()) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Document ID is required for DELETE");
            return createResponse(httpRequest, 400, error);
        }

        final Map<String, Object> vespaResult = client.delete(indexName, documentType, docId);

        final Map<String, Object> result = new HashMap<>();
        result.put("_index", indexName);
        result.put("_id", docId);
        result.put("_version", 1);
        result.put("result", "deleted");
        result.put("_shards", Map.of("total", 1, "successful", 1, "failed", 0));

        return createResponse(httpRequest, 200, result);
    }

}
