package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class BulkActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private BulkAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new BulkAction(handler);
    }

    @Test
    void testBulkIndexOperations() throws IOException {
        // Prepare bulk request with index operations
        String bulkRequest = "{\"index\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"Document 1\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_id\":\"2\"}}\n" +
                "{\"title\":\"Document 2\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(eq("test"), eq("doc"), eq("1"), anyMap())).thenReturn(Map.of());
        when(vespaClient.insert(eq("test"), eq("doc"), eq("2"), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(2, items.size());

        verify(vespaClient, times(2)).insert(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    void testBulkCreateOperations() throws IOException {
        // Prepare bulk request with create operations
        String bulkRequest = "{\"create\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"New Document 1\"}\n" +
                "{\"create\":{\"_index\":\"test\",\"_id\":\"2\"}}\n" +
                "{\"title\":\"New Document 2\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(2, items.size());
    }

    @Test
    void testBulkUpdateOperations() throws IOException {
        // Prepare bulk request with update operations
        String bulkRequest = "{\"update\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"Updated Document 1\"}\n" +
                "{\"update\":{\"_index\":\"test\",\"_id\":\"2\"}}\n" +
                "{\"title\":\"Updated Document 2\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.update(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(2, items.size());

        verify(vespaClient, times(2)).update(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    void testBulkDeleteOperations() throws IOException {
        // Prepare bulk request with delete operations
        String bulkRequest = "{\"delete\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"delete\":{\"_index\":\"test\",\"_id\":\"2\"}}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.delete(anyString(), anyString(), anyString())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(2, items.size());

        verify(vespaClient, times(2)).delete(anyString(), anyString(), anyString());
    }

    @Test
    void testBulkMixedOperations() throws IOException {
        // Prepare bulk request with mixed operations
        String bulkRequest = "{\"index\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"Document 1\"}\n" +
                "{\"create\":{\"_index\":\"test\",\"_id\":\"2\"}}\n" +
                "{\"title\":\"Document 2\"}\n" +
                "{\"update\":{\"_index\":\"test\",\"_id\":\"3\"}}\n" +
                "{\"title\":\"Updated Document 3\"}\n" +
                "{\"delete\":{\"_index\":\"test\",\"_id\":\"4\"}}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());
        when(vespaClient.update(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());
        when(vespaClient.delete(anyString(), anyString(), anyString())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(4, items.size());
    }

    @Test
    void testBulkWithDefaultIndex() throws IOException {
        // Prepare bulk request without index specified (should use default from URL)
        String bulkRequest = "{\"index\":{\"_id\":\"1\"}}\n" +
                "{\"title\":\"Document 1\"}\n" +
                "{\"index\":{\"_id\":\"2\"}}\n" +
                "{\"title\":\"Document 2\"}\n";

        HttpRequest request = createMockRequest("POST", "/myindex/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(eq("myindex"), eq("doc"), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        verify(vespaClient, times(2)).insert(eq("myindex"), eq("doc"), anyString(), anyMap());
    }

    @Test
    void testBulkWithAutoGeneratedIds() throws IOException {
        // Prepare bulk request without IDs (should auto-generate)
        String bulkRequest = "{\"index\":{\"_index\":\"test\"}}\n" +
                "{\"title\":\"Document 1\"}\n" +
                "{\"index\":{\"_index\":\"test\"}}\n" +
                "{\"title\":\"Document 2\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));
    }

    @Test
    void testBulkWithPartialErrors() throws IOException {
        // Prepare bulk request
        String bulkRequest = "{\"index\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"Document 1\"}\n" +
                "{\"update\":{\"_index\":\"test\"}}\n" +  // Missing ID, should error
                "{\"title\":\"Document 2\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_id\":\"3\"}}\n" +
                "{\"title\":\"Document 3\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock VespaClient responses
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertTrue((Boolean) responseBody.get("errors")); // Should have errors

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(3, items.size());
    }

    @Test
    void testBulkWithVespaClientException() throws IOException {
        // Prepare bulk request
        String bulkRequest = "{\"index\":{\"_index\":\"test\",\"_id\":\"1\"}}\n" +
                "{\"title\":\"Document 1\"}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Mock exception
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap()))
                .thenThrow(new VespaClientException("Vespa connection failed"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertTrue((Boolean) responseBody.get("errors")); // Should have errors
    }

    @Test
    void testBulkWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        String bulkRequest = "{invalid json\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testBulkWithEmptyRequest() throws IOException {
        // Prepare empty request
        String bulkRequest = "";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(0, items.size());
    }

    @Test
    void testBulkWithManyOperations() throws IOException {
        // Prepare bulk request with many operations
        StringBuilder bulkRequest = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            bulkRequest.append("{\"index\":{\"_index\":\"test\",\"_id\":\"").append(i).append("\"}}\n");
            bulkRequest.append("{\"title\":\"Document ").append(i).append("\"}\n");
        }

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest.toString());

        // Mock VespaClient responses
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(Map.of());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertFalse((Boolean) responseBody.get("errors"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) responseBody.get("items");
        assertEquals(100, items.size());

        verify(vespaClient, times(100)).insert(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    void testBulkDeleteWithoutId() throws IOException {
        // Prepare bulk delete without ID (should error)
        String bulkRequest = "{\"delete\":{\"_index\":\"test\"}}\n";

        HttpRequest request = createMockRequest("POST", "/_bulk", bulkRequest);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertTrue((Boolean) responseBody.get("errors")); // Should have errors
    }

    private HttpRequest createMockRequest(String method, String path, String body) throws IOException {
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost" + path));
        when(request.getData()).thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));

        return request;
    }

    private Map<String, Object> parseResponse(HttpResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        response.render(baos);
        String json = baos.toString(StandardCharsets.UTF_8);

        if (json.trim().isEmpty()) {
            return new HashMap<>();
        }

        return JsonXContent.jsonXContent
                .createParser(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))
                .map();
    }
}
