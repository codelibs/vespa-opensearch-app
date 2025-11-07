package org.codelibs.vespa.opensearch.action;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class BulkAction extends HttpAction {

    public BulkAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        // POST /_bulk
        // POST /<index>/_bulk
        if (method == Method.POST || method == Method.PUT) {
            if (paths.length == 2 && "_bulk".equals(paths[1])) {
                return true;
            }
            if (paths.length == 3 && !paths[1].startsWith("_") && "_bulk".equals(paths[2])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        final String[] paths = path.split("/");
        final String defaultIndex = paths.length == 3 ? paths[1] : null;

        final VespaClient client = handler.getVespaClient();
        final String documentType = handler.getDocumentType();

        final List<Map<String, Object>> items = new ArrayList<>();
        boolean hasErrors = false;
        int took = 0;

        try (InputStream is = httpRequest.getData(); BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            Map<String, Object> action = null;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) {
                    continue;
                }

                if (lineNum % 2 == 1) {
                    // Process previous action if it exists (for delete operations without document body)
                    if (action != null) {
                        final Map<String, Object> result = processBulkAction(action, new HashMap<>(), defaultIndex, client, documentType);
                        items.add(result);
                        if (result.containsKey("error")) {
                            hasErrors = true;
                        }
                    }

                    // Action line
                    action = JsonXContent.jsonXContent
                            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, line).map();
                } else {
                    // Document line
                    if (action == null) {
                        continue;
                    }

                    final Map<String, Object> doc = JsonXContent.jsonXContent
                            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, line).map();

                    final Map<String, Object> result = processBulkAction(action, doc, defaultIndex, client, documentType);
                    items.add(result);

                    if (result.containsKey("error")) {
                        hasErrors = true;
                    }

                    action = null;
                }
            }

            // Process final action if it exists (for delete operations without document body)
            if (action != null) {
                final Map<String, Object> result = processBulkAction(action, new HashMap<>(), defaultIndex, client, documentType);
                items.add(result);
                if (result.containsKey("error")) {
                    hasErrors = true;
                }
            }
        } catch (final IOException e) {
            final Map<String, Object> error = new HashMap<>();
            error.put("error", "Failed to parse bulk request: " + e.getMessage());
            return createResponse(httpRequest, 400, error);
        }

        final Map<String, Object> result = new HashMap<>();
        result.put("took", took);
        result.put("errors", hasErrors);
        result.put("items", items);

        return createResponse(httpRequest, 200, result);
    }

    private Map<String, Object> processBulkAction(final Map<String, Object> action, final Map<String, Object> doc,
            final String defaultIndex, final VespaClient client, final String documentType) {
        final Map<String, Object> result = new HashMap<>();

        try {
            if (action.containsKey("index")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> indexAction = (Map<String, Object>) action.get("index");
                final String index = (String) indexAction.getOrDefault("_index", defaultIndex);
                final String id = (String) indexAction.getOrDefault("_id", UUID.randomUUID().toString());

                client.insert(index, documentType, id, doc);

                final Map<String, Object> indexResult = new HashMap<>();
                indexResult.put("_index", index);
                indexResult.put("_id", id);
                indexResult.put("_version", 1);
                indexResult.put("result", "created");
                indexResult.put("status", 201);
                result.put("index", indexResult);

            } else if (action.containsKey("create")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> createAction = (Map<String, Object>) action.get("create");
                final String index = (String) createAction.getOrDefault("_index", defaultIndex);
                final String id = (String) createAction.getOrDefault("_id", UUID.randomUUID().toString());

                client.insert(index, documentType, id, doc);

                final Map<String, Object> createResult = new HashMap<>();
                createResult.put("_index", index);
                createResult.put("_id", id);
                createResult.put("_version", 1);
                createResult.put("result", "created");
                createResult.put("status", 201);
                result.put("create", createResult);

            } else if (action.containsKey("update")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> updateAction = (Map<String, Object>) action.get("update");
                final String index = (String) updateAction.getOrDefault("_index", defaultIndex);
                final String id = (String) updateAction.get("_id");

                if (id == null) {
                    throw new IllegalArgumentException("Document ID is required for update");
                }

                client.update(index, documentType, id, doc);

                final Map<String, Object> updateResult = new HashMap<>();
                updateResult.put("_index", index);
                updateResult.put("_id", id);
                updateResult.put("_version", 1);
                updateResult.put("result", "updated");
                updateResult.put("status", 200);
                result.put("update", updateResult);

            } else if (action.containsKey("delete")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> deleteAction = (Map<String, Object>) action.get("delete");
                final String index = (String) deleteAction.getOrDefault("_index", defaultIndex);
                final String id = (String) deleteAction.get("_id");

                if (id == null) {
                    throw new IllegalArgumentException("Document ID is required for delete");
                }

                client.delete(index, documentType, id);

                final Map<String, Object> deleteResult = new HashMap<>();
                deleteResult.put("_index", index);
                deleteResult.put("_id", id);
                deleteResult.put("_version", 1);
                deleteResult.put("result", "deleted");
                deleteResult.put("status", 200);
                result.put("delete", deleteResult);
            }
        } catch (final Exception e) {
            final Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("error", Map.of("type", "exception", "reason", e.getMessage()));
            errorResult.put("status", 400);
            result.put(action.keySet().iterator().next(), errorResult);
        }

        return result;
    }

}
