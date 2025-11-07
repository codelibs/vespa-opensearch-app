package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class MgetActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private MgetAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new MgetAction(handler);
    }

    @Test
    void testMgetWithMultipleIds() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("ids", List.of("doc1", "doc2", "doc3"));
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "docs", List.of(
                        Map.of("_id", "doc1", "_found", true, "_source", Map.of("title", "Document 1")),
                        Map.of("_id", "doc2", "_found", true, "_source", Map.of("title", "Document 2")),
                        Map.of("_id", "doc3", "_found", true, "_source", Map.of("title", "Document 3"))));
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
    }

    @Test
    void testMgetWithSpecificIndex() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("ids", List.of("product1", "product2"));
        HttpRequest request = createMockRequest("POST", "/products/_mget", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "docs", List.of(
                        Map.of("_id", "product1", "_found", true, "_source", Map.of("name", "Product 1", "price", 100)),
                        Map.of("_id", "product2", "_found", true, "_source", Map.of("name", "Product 2", "price", 200))));
        when(vespaClient.multiGet(eq("products"), eq("doc"), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithSingleId() throws IOException {
        // Prepare request with single ID
        Map<String, Object> requestBody = Map.of("ids", List.of("doc1"));
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "docs", List.of(
                        Map.of("_id", "doc1", "_found", true, "_source", Map.of("title", "Document 1"))));
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithSomeNotFound() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("ids", List.of("doc1", "doc2", "doc3"));
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response with some not found
        Map<String, Object> vespaResponse = Map.of(
                "docs", List.of(
                        Map.of("_id", "doc1", "_found", true, "_source", Map.of("title", "Document 1")),
                        Map.of("_id", "doc2", "_found", false),
                        Map.of("_id", "doc3", "_found", true, "_source", Map.of("title", "Document 3"))));
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithEmptyIds() throws IOException {
        // Prepare request with empty IDs list
        Map<String, Object> requestBody = Map.of("ids", List.of());
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("docs", List.of());
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithoutIdsField() throws IOException {
        // Prepare request without 'ids' field
        Map<String, Object> requestBody = Map.of("other_field", "value");
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody.get("error"));
    }

    @Test
    void testMgetWithNullBody() throws IOException {
        // Prepare request with null body
        HttpRequest request = createMockRequest("POST", "/_mget", null);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testMgetWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost/_mget"));
        when(request.getData()).thenReturn(new ByteArrayInputStream("{invalid json".getBytes()));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testMgetWithVespaClientException() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("ids", List.of("doc1", "doc2"));
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock exception
        when(vespaClient.multiGet(anyString(), anyString(), anyList()))
                .thenThrow(new VespaClientException("Vespa connection failed"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 500
        assertEquals(500, response.getStatus());
    }

    @Test
    void testMgetWithManyIds() throws IOException {
        // Prepare request with many IDs
        List<String> manyIds = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            manyIds.add("doc" + i);
        }
        Map<String, Object> requestBody = Map.of("ids", manyIds);
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            docs.add(Map.of("_id", "doc" + i, "_found", true, "_source", Map.of("title", "Document " + i)));
        }
        Map<String, Object> vespaResponse = Map.of("docs", docs);
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithSpecialCharactersInIds() throws IOException {
        // Prepare request with special characters in IDs
        Map<String, Object> requestBody = Map.of("ids", List.of("doc-1", "doc_2", "doc:3", "doc@4"));
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "docs", List.of(
                        Map.of("_id", "doc-1", "_found", true, "_source", Map.of("title", "Document 1")),
                        Map.of("_id", "doc_2", "_found", true, "_source", Map.of("title", "Document 2")),
                        Map.of("_id", "doc:3", "_found", true, "_source", Map.of("title", "Document 3")),
                        Map.of("_id", "doc@4", "_found", true, "_source", Map.of("title", "Document 4"))));
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
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
