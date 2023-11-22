package org.codelibs.vespa.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class VespaClientTests {

    static final String version = "8.256.22";

    static final String imageTag = "vespaengine/vespa:" + version;

    static GenericContainer server;

    private VespaClient client;

    private static final Logger log = Logger.getLogger(VespaClientTests.class.getName());;

    @BeforeAll
    static void setUpAll() throws IOException {
        startServer();
        waitFor("http://" + server.getHost() + ":" + server.getFirstMappedPort());
        deployApp();
        waitFor("http://" + server.getHost() + ":" + server.getMappedPort(8080));
    }

    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse(imageTag))//
                .withExposedPorts(19071, 8080).waitingFor(new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {
                        // nothing
                    }

                    @Override
                    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                        return null;
                    }
                });
        server.start();
    }

    static void waitFor(final String url) {
        log.log(Level.INFO, () -> "Vespa " + version + ": " + url);
        for (int i = 0; i < 60; i++) {
            try (CurlResponse response = Curl.get(url).execute()) {
                if (response.getHttpStatusCode() == 200) {
                    log.log(Level.INFO, () -> url + " is available.");
                    break;
                }
            } catch (final Exception e) {
                log.log(Level.WARNING, () -> e.getLocalizedMessage());
            }
            try {
                final int count = i + 1;
                log.log(Level.INFO, () -> "[" + count + "] Waiting for " + url);
                Thread.sleep(1000L);
            } catch (final InterruptedException e) {
                // nothing
            }
        }
    }

    static void deployApp() throws IOException {
        File zipFile = File.createTempFile("app", ".zip");
        try {
            zipDirectory(new File("src/test/resources/app"), zipFile);

            final String url =
                    "http://" + server.getHost() + ":" + server.getFirstMappedPort() + "/application/v2/tenant/default/prepareandactivate";
            try (InputStream is = new FileInputStream(zipFile);
                    CurlResponse response = Curl.post(url).header("Content-Type", "application/zip").body(is).execute()) {
                if (response.getHttpStatusCode() == 200) {
                    log.log(Level.INFO, () -> "Application is deployed.");
                }
            }
        } finally {
            if (zipFile.exists()) {
                zipFile.delete();
            }
        }
    }

    static void zipDirectory(File dir, File zipFile) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile))) {
            Files.walk(Paths.get(dir.getPath())).filter(path -> !Files.isDirectory(path)).forEach(path -> {
                ZipEntry zipEntry = new ZipEntry(dir.toPath().relativize(path).toString());
                try {
                    zos.putNextEntry(zipEntry);
                    Files.copy(path, zos);
                    zos.closeEntry();
                } catch (IOException e) {
                    throw new RuntimeException("Error zipping file: " + path, e);
                }
            });
        }
    }

    @BeforeEach
    void setUp() {
        client = new VespaClient("http://" + server.getHost() + ":" + server.getMappedPort(8080));
    }

    @AfterEach
    void tearDown() {
        log.log(Level.INFO, () -> "Closing client");
    }

    @AfterAll
    static void tearDownAll() {
        server.stop();
    }

    @Test
    public void testGetRoot() {
        Map<String, Object> info = client.getInfo();

        assertNotNull(info);
    }
}
