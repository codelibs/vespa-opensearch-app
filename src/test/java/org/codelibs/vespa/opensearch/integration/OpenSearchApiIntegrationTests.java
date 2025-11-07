package org.codelibs.vespa.opensearch.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests that require a running Vespa instance at localhost:8080.
 * These tests are disabled by default. To run them, remove the @Disabled annotation
 * and ensure a Vespa instance is running.
 */
@Disabled("Integration tests require a running Vespa instance at localhost:8080")
public class OpenSearchApiIntegrationTests {

    private VespaClient client;

    @BeforeEach
    void setUp() {
        client = new VespaClient("http://localhost:8080");
    }

    @Test
    void testIndexOperations() {
        final String indexName = "test_index";

        // Create index
        Map<String, Object> createResponse = client.createIndex(indexName, Map.of("number_of_shards", 1));
        assertNotNull(createResponse);
        assertTrue((Boolean) createResponse.get("acknowledged"));
        assertEquals(indexName, createResponse.get("index"));

        // Check index exists
        assertTrue(client.indexExists(indexName));

        // Get index
        Map<String, Object> indexInfo = client.getIndex(indexName);
        assertNotNull(indexInfo);
        assertTrue(indexInfo.containsKey(indexName));

        // Update settings
        Map<String, Object> updateSettingsResponse = client.updateSettings(indexName, Map.of("refresh_interval", "30s"));
        assertTrue((Boolean) updateSettingsResponse.get("acknowledged"));

        // Get settings
        Map<String, Object> settings = client.getSettings(indexName);
        assertNotNull(settings);
        assertTrue(settings.containsKey(indexName));

        // Delete index
        Map<String, Object> deleteResponse = client.deleteIndex(indexName);
        assertTrue((Boolean) deleteResponse.get("acknowledged"));

        // Check index no longer exists
        assertFalse(client.indexExists(indexName));
    }

    @Test
    void testMappingOperations() {
        final String indexName = "test_mapping_index";

        // Create index first
        client.createIndex(indexName, null);

        // Update mapping
        Map<String, Object> mapping = Map.of(
                "properties", Map.of(
                        "title", Map.of("type", "text"),
                        "content", Map.of("type", "text")));

        Map<String, Object> updateResponse = client.updateMapping(indexName, mapping);
        assertTrue((Boolean) updateResponse.get("acknowledged"));

        // Get mapping
        Map<String, Object> mappingResponse = client.getMapping(indexName);
        assertNotNull(mappingResponse);
        assertTrue(mappingResponse.containsKey(indexName));

        // Cleanup
        client.deleteIndex(indexName);
    }

    @Test
    void testClusterOperations() {
        // Get cluster health
        Map<String, Object> health = client.getClusterHealth();
        assertNotNull(health);
        assertEquals("vespa-cluster", health.get("cluster_name"));
        assertEquals("green", health.get("status"));
        assertFalse((Boolean) health.get("timed_out"));

        // Get cluster state
        Map<String, Object> state = client.getClusterState();
        assertNotNull(state);
        assertEquals("vespa-cluster", state.get("cluster_name"));
        assertTrue(state.containsKey("metadata"));
    }

    @Test
    void testDocumentOperations() {
        final String namespace = "test_ns";
        final String docType = "doc";
        final String docId = "test_doc_1";

        // Insert document
        Map<String, Object> doc = Map.of(
                "title", "Test Document",
                "content", "This is a test document");

        Map<String, Object> insertResponse = client.insert(namespace, docType, docId, doc);
        assertNotNull(insertResponse);

        // Get document
        Map<String, Object> getResponse = client.get(namespace, docType, docId);
        assertNotNull(getResponse);
        assertTrue(getResponse.containsKey("fields"));

        // Update document
        Map<String, Object> updatedDoc = Map.of(
                "title", "Updated Test Document",
                "content", "This is an updated test document");

        Map<String, Object> updateResponse = client.update(namespace, docType, docId, updatedDoc);
        assertNotNull(updateResponse);

        // Delete document
        Map<String, Object> deleteResponse = client.delete(namespace, docType, docId);
        assertNotNull(deleteResponse);
    }

    @Test
    void testGetAllIndices() {
        // Create multiple indices
        client.createIndex("index1", null);
        client.createIndex("index2", null);

        // Get all indices
        Map<String, Map<String, Object>> indices = client.getAllIndices();
        assertNotNull(indices);
        assertTrue(indices.size() >= 2);
        assertTrue(indices.containsKey("index1"));
        assertTrue(indices.containsKey("index2"));

        // Cleanup
        client.deleteIndex("index1");
        client.deleteIndex("index2");
    }
}
