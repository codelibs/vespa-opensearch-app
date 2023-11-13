Vespa OpenSearch Proxy Application
==================================

## Overview

This project develops a sample application for deploying to the Vespa search engine, providing compatibility with Elasticsearch and OpenSearch request formats.
It aims to bridge the gap between Vespa's advanced search capabilities and the popular Elasticsearch/OpenSearch ecosystems.

Note: This application is currently under development and may be subject to changes.

## Usage

### Build

```
$ mvn package
```

### Start Vespa

```
$ docker run --detach --name vespa --hostname vespa-container --publish 8080:8080 --publish 19071:19071 vespaengine/vespa
```

### Deploy Application

```
$ curl --header Content-Type:application/zip --data-binary @target/application.zip localhost:19071/application/v2/tenant/default/prepareandactivate
```

