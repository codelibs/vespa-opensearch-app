package org.codelibs.vespa.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryDSLTranslationTests {

    private VespaClient client;
    private Method buildYqlMethod;

    @BeforeEach
    void setUp() throws Exception {
        client = new VespaClient("http://localhost:8080");
        // Access private method for testing
        buildYqlMethod = VespaClient.class.getDeclaredMethod("buildYqlFromOpenSearchQuery", Map.class);
        buildYqlMethod.setAccessible(true);
    }

    private String buildYql(Map<String, Object> searchRequest) throws Exception {
        return (String) buildYqlMethod.invoke(client, searchRequest);
    }

    @Test
    void testMatchAllQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("match_all", Map.of()));
        final String yql = buildYql(searchRequest);
        assertEquals("select * from sources * where true", yql);
    }

    @Test
    void testMatchQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("match", Map.of("title", "hello world")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains \"hello world\""));
    }

    @Test
    void testMatchPhraseQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("match_phrase", Map.of("title", "hello world")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains phrase(\"hello world\")"));
    }

    @Test
    void testTermQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("term", Map.of("status", "published")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("status matches \"published\""));
    }

    @Test
    void testTermsQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("terms", Map.of("status", List.of("published", "draft"))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("status matches \"published\" OR status matches \"draft\""));
    }

    @Test
    void testRangeQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("range", Map.of("age", Map.of("gte", 18, "lte", 65))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("age >= 18"));
        assertTrue(yql.contains("age <= 65"));
    }

    @Test
    void testPrefixQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("prefix", Map.of("title", "test")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title matches \"test*\""));
    }

    @Test
    void testWildcardQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("wildcard", Map.of("title", "te*t")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title matches \"te*t\""));
    }

    @Test
    void testExistsQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("exists", Map.of("field", "title")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title"));
    }

    @Test
    void testBoolQueryMust() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("bool", Map.of("must", List.of(Map.of("match", Map.of("title", "hello")),
                        Map.of("match", Map.of("content", "world"))))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains \"hello\""));
        assertTrue(yql.contains("content contains \"world\""));
        assertTrue(yql.contains("AND"));
    }

    @Test
    void testBoolQueryShould() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("bool", Map.of("should", List.of(Map.of("match", Map.of("title", "hello")),
                        Map.of("match", Map.of("content", "world"))))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains \"hello\""));
        assertTrue(yql.contains("content contains \"world\""));
        assertTrue(yql.contains("OR"));
    }

    @Test
    void testBoolQueryMustNot() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("bool", Map.of("must_not", List.of(Map.of("match", Map.of("status", "deleted"))))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("!"));
        assertTrue(yql.contains("status contains \"deleted\""));
    }

    @Test
    void testComplexBoolQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("bool", Map.of("must", List.of(Map.of("match", Map.of("title", "hello"))), "should",
                        List.of(Map.of("match", Map.of("content", "world")), Map.of("match", Map.of("description", "test"))),
                        "must_not", List.of(Map.of("term", Map.of("status", "deleted"))))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains \"hello\""));
        assertTrue(yql.contains("content contains \"world\""));
        assertTrue(yql.contains("description contains \"test\""));
        assertTrue(yql.contains("status matches \"deleted\""));
    }

    @Test
    void testMultiMatchQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query",
                Map.of("multi_match", Map.of("query", "hello", "fields", List.of("title", "content"))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("title contains \"hello\""));
        assertTrue(yql.contains("content contains \"hello\""));
        assertTrue(yql.contains("OR"));
    }

    @Test
    void testIdsQuery() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("ids", Map.of("values", List.of("1", "2", "3"))));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("documentid"));
    }

    @Test
    void testEscaping() throws Exception {
        final Map<String, Object> searchRequest = Map.of("query", Map.of("match", Map.of("title", "test \"quote\"")));
        final String yql = buildYql(searchRequest);
        assertTrue(yql.contains("\\\""));
    }
}
