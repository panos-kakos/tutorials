package com.baeldung.javahttpclient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.baeldung.Post;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class JavaHttpClientPostServiceIntegrationTest {

    static WireMockServer wireMockServer;

    @Inject
    JavaHttpClientPostService javaHttpClientPostService;

    @Inject
    ObjectMapper objectMapper;

    @BeforeAll
    static void setup() {
        // Start WireMock on a random available port
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();

        // Configure WireMock to respond to the API endpoint
        wireMockServer.stubFor(get(urlEqualTo("/posts")).willReturn(aResponse().withHeader("Content-Type", "application/json")
          .withBody("[{\"id\":1,\"title\":\"Post Title 1\",\"description\":\"Post description 1\"}]")));

        // Set system property
        System.setProperty("quarkus.rest-client.post-rest-client.url", wireMockServer.baseUrl());
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    public void whenCallingHttpClientPostService_thenPostsAreReturned() throws JsonProcessingException {
        // Call the service method
        List<Post> posts = javaHttpClientPostService.getPosts();

        // Validate the response
        assertNotNull(posts);
        assertFalse(posts.isEmpty(), "Expected at least one post");
        assertEquals("[{\"id\":1,\"title\":\"Post Title 1\",\"description\":\"Post description 1\"}]", objectMapper.writeValueAsString(posts));
    }

}