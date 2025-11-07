Vespa OpenSearch Proxy Application
==================================

## Overview

This project provides a comprehensive OpenSearch-compatible API proxy for the Vespa search engine. It allows applications using the OpenSearch/Elasticsearch client libraries to seamlessly interact with Vespa as if it were an OpenSearch cluster.

The proxy translates OpenSearch API requests into Vespa operations, providing compatibility between Vespa's advanced search capabilities and the popular OpenSearch/Elasticsearch ecosystems.

## Features

### Index Operations
- **Create Index**: `PUT /<index>` - Create a new index with optional settings and mappings
- **Delete Index**: `DELETE /<index>` - Remove an index
- **Get Index**: `GET /<index>` - Retrieve index information
- **Index Exists**: `HEAD /<index>` - Check if an index exists
- **List Indices**: `GET /_cat/indices` - List all indices

### Document Operations
- **Index Document**: `POST /<index>/_doc` or `POST /<index>/_doc/<id>` - Add a document with auto-generated or specified ID
- **Create Document**: `POST /<index>/_create/<id>` or `PUT /<index>/_create/<id>` - Create a document (fails if exists)
- **Update Document**: `PUT /<index>/_doc/<id>` - Update an existing document
- **Get Document**: `GET /<index>/_doc/<id>` - Retrieve a document by ID
- **Delete Document**: `DELETE /<index>/_doc/<id>` - Remove a document

### Bulk Operations
- **Bulk API**: `POST /_bulk` or `POST /<index>/_bulk` - Perform multiple index/create/update/delete operations in a single request

### Search Operations
- **Search**: `GET/POST /<index>/_search` or `GET/POST /_search` - Search for documents using OpenSearch query DSL
- **Count**: `GET/POST /<index>/_count` or `GET/POST /_count` - Count documents matching a query
- **Multi Get**: `GET/POST /<index>/_mget` or `GET/POST /_mget` - Retrieve multiple documents by IDs

### Advanced Document Operations
- **Partial Update**: `POST /<index>/_update/<id>` - Update specific fields of a document
- **Refresh**: `POST /<index>/_refresh` or `POST /_refresh` - Refresh the index (no-op for Vespa, returns success)

### Cluster Information
- **Cluster Health**: `GET /_cluster/health` - Get cluster health status
- **Cluster State**: `GET /_cluster/state` - Get cluster state information
- **Root Info**: `GET /` - Get basic cluster and version information

### Index Settings and Mappings
- **Get Mapping**: `GET /<index>/_mapping` - Retrieve index mappings
- **Update Mapping**: `PUT /<index>/_mapping` - Update index mappings
- **Get Settings**: `GET /<index>/_settings` - Retrieve index settings
- **Update Settings**: `PUT /<index>/_settings` - Update index settings

## Architecture

The application consists of several key components:

- **RestApiProxyHandler**: Main HTTP request handler that routes requests to appropriate actions
- **VespaClient**: Client for communicating with Vespa's Document API
- **Action Classes**: Individual handlers for different OpenSearch API endpoints
  - `RootAction`: Root endpoint (/)
  - `ClusterHealthAction`: Cluster health endpoint
  - `ClusterStateAction`: Cluster state endpoint
  - `IndicesAction`: Index management operations
  - `DocumentAction`: Document CRUD operations
  - `MappingAction`: Index mapping operations
  - `SettingsAction`: Index settings operations
  - `BulkAction`: Bulk operations
  - `CatIndicesAction`: Indices listing
  - `SearchAction`: Search operations
  - `CountAction`: Document count operations
  - `MgetAction`: Multi-document get operations
  - `UpdateAction`: Partial document updates
  - `RefreshAction`: Index refresh operations

## Usage

### Build

```bash
mvn package
```

### Start Vespa

```bash
docker run --detach --name vespa --hostname vespa-container \
  --publish 8080:8080 --publish 19071:19071 vespaengine/vespa
```

### Deploy Application

```bash
curl --header Content-Type:application/zip \
  --data-binary @target/application.zip \
  localhost:19071/application/v2/tenant/default/prepareandactivate
```

### Example Requests

#### Create an index
```bash
curl -X PUT "localhost:8080/opensearch/myindex" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "content": { "type": "text" }
    }
  }
}'
```

#### Index a document
```bash
curl -X POST "localhost:8080/opensearch/myindex/_doc/1" -H 'Content-Type: application/json' -d'
{
  "title": "Hello Vespa",
  "content": "This is a test document"
}'
```

#### Get a document
```bash
curl -X GET "localhost:8080/opensearch/myindex/_doc/1"
```

#### Delete a document
```bash
curl -X DELETE "localhost:8080/opensearch/myindex/_doc/1"
```

#### Bulk operations
```bash
curl -X POST "localhost:8080/opensearch/_bulk" -H 'Content-Type: application/json' -d'
{"index":{"_index":"myindex","_id":"1"}}
{"title":"Document 1","content":"First document"}
{"index":{"_index":"myindex","_id":"2"}}
{"title":"Document 2","content":"Second document"}
'
```

#### Check cluster health
```bash
curl -X GET "localhost:8080/opensearch/_cluster/health"
```

#### Search for documents
```bash
curl -X POST "localhost:8080/opensearch/myindex/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "title": "Hello"
    }
  },
  "size": 10
}'
```

#### Count documents
```bash
curl -X GET "localhost:8080/opensearch/myindex/_count" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match_all": {}
  }
}'
```

#### Get multiple documents
```bash
curl -X POST "localhost:8080/opensearch/myindex/_mget" -H 'Content-Type: application/json' -d'
{
  "ids": ["1", "2", "3"]
}'
```

#### Partial update
```bash
curl -X POST "localhost:8080/opensearch/myindex/_update/1" -H 'Content-Type: application/json' -d'
{
  "doc": {
    "title": "Updated Title"
  }
}'
```

#### Refresh index
```bash
curl -X POST "localhost:8080/opensearch/myindex/_refresh"
```

## Testing

The project includes comprehensive unit and integration tests:

### Unit Tests
- Action routing tests for all endpoint handlers
- Path matching validation

### Integration Tests
- Full API workflow tests
- Index lifecycle management
- Document CRUD operations
- Cluster information retrieval

Run tests with:
```bash
mvn test
```

## Configuration

The application can be configured through `services.xml`:

```xml
<config name="org.codelibs.vespa.opensearch.config.proxy-handler">
  <vespaEndpoint>http://localhost:8080</vespaEndpoint>
  <documentType>doc</documentType>
  <pathPrefix>/opensearch</pathPrefix>
</config>
```

## Limitations

- Index metadata (settings, mappings) is stored in-memory as Vespa schemas are static
- Search query translation supports basic queries (match_all, match, term) - complex query DSL features are not yet fully supported
- Some advanced OpenSearch features may not be fully supported (e.g., aggregations, suggesters, percolate queries)

## License

This project is under development. License information will be provided in future releases.

## Contributing

Contributions are welcome! Please submit pull requests or open issues for bugs and feature requests.

