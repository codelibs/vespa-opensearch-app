package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class UpdateActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private UpdateAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new UpdateAction(handler);
    }

    @Test
    void testUpdateWithDoc() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of(
                        "title", "Updated Title",
                        "content", "Updated Content"));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(eq("myindex"), eq("doc"), eq("doc1"), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
        assertEquals("myindex", responseBody.get("_index"));
        assertEquals("doc1", responseBody.get("_id"));
        assertEquals("updated", responseBody.get("result"));

        verify(vespaClient, times(1)).partialUpdate(eq("myindex"), eq("doc"), eq("doc1"), anyMap());
    }

    @Test
    void testUpdateWithScript() throws IOException {
        // Prepare request with script
        Map<String, Object> requestBody = Map.of(
                "script", Map.of(
                        "source", "ctx._source.counter += params.count",
                        "params", Map.of("count", 4)));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        verify(vespaClient, times(1)).partialUpdate(eq("myindex"), eq("doc"), eq("doc1"), anyMap());
    }

    @Test
    void testUpdatePartialFields() throws IOException {
        // Prepare request with partial field update
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of("status", "published"));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testUpdateMultipleFields() throws IOException {
        // Prepare request with multiple fields
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of(
                        "title", "New Title",
                        "content", "New Content",
                        "status", "published",
                        "views", 100));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testUpdateWithUpsert() throws IOException {
        // Prepare request with upsert
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of("title", "Updated Title"),
                "doc_as_upsert", true);
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testUpdateWithoutBody() throws IOException {
        // Prepare request without body
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", null);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody.get("error"));
    }

    @Test
    void testUpdateWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost/myindex/_update/doc1"));
        when(request.getData()).thenReturn(new ByteArrayInputStream("{invalid json".getBytes()));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testUpdateWithVespaClientException() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("doc", Map.of("title", "Updated Title"));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock exception
        when(vespaClient.partialUpdate(anyString(), anyString(), anyString(), anyMap()))
                .thenThrow(new VespaClientException("Document not found"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 404
        assertEquals(404, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody.get("error"));
    }

    @Test
    void testUpdateWithSpecialCharactersInId() throws IOException {
        // Prepare request with special characters in ID
        Map<String, Object> requestBody = Map.of("doc", Map.of("title", "Updated Title"));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc-1:2:3", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), eq("doc-1:2:3"), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertEquals("doc-1:2:3", responseBody.get("_id"));
    }

    @Test
    void testUpdateWithNestedFields() throws IOException {
        // Prepare request with nested fields
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of(
                        "user", Map.of(
                                "name", "John Doe",
                                "email", "john@example.com")));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testUpdateWithArrayFields() throws IOException {
        // Prepare request with array fields
        Map<String, Object> requestBody = Map.of(
                "doc", Map.of("tags", java.util.List.of("tag1", "tag2", "tag3")));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testUpdateDifferentIndexes() throws IOException {
        // Test update on different indexes
        String[] indexes = {"index1", "index2", "products", "users"};

        // Setup mock to accept any index
        doNothing().when(vespaClient).partialUpdate(anyString(), eq("doc"), eq("doc1"), anyMap());

        for (String index : indexes) {
            Map<String, Object> requestBody = Map.of("doc", Map.of("title", "Updated"));
            HttpRequest request = createMockRequest("POST", "/" + index + "/_update/doc1", requestBody);

            HttpResponse response = action.execute(request);

            assertEquals(200, response.getStatus());
            Map<String, Object> responseBody = parseResponse(response);
            assertEquals(index, responseBody.get("_index"));
        }
    }

    @Test
    void testUpdateResponseContainsShards() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("doc", Map.of("title", "Updated"));
        HttpRequest request = createMockRequest("POST", "/myindex/_update/doc1", requestBody);

        // Mock VespaClient response
        doNothing().when(vespaClient).partialUpdate(anyString(), anyString(), anyString(), anyMap());

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);

        @SuppressWarnings("unchecked")
        Map<String, Object> shards = (Map<String, Object>) responseBody.get("_shards");
        assertNotNull(shards);
        assertEquals(1, shards.get("total"));
        assertEquals(1, shards.get("successful"));
        assertEquals(0, shards.get("failed"));
    }

    private HttpRequest createMockRequest(String method, String path, Map<String, Object> body) throws IOException {
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost" + path));

        if (body != null) {
            // Convert map to JSON
            byte[] jsonBytes = new ObjectMapper().writeValueAsBytes(body);
            when(request.getData()).thenReturn(new ByteArrayInputStream(jsonBytes));
        } else {
            when(request.getData()).thenReturn(new ByteArrayInputStream(new byte[0]));
        }

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
