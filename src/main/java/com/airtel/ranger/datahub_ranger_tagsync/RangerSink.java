package com.vijayjangir.ranger.datahub_ranger_tagsync;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.concurrent.*;

public class RangerSink {
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY = 1; // delay in seconds
    private final HttpClient client = HttpClient.newHttpClient();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    public CompletableFuture<HttpResponse<String>> pushTags(String payload) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(Constants.RANGER_TAG_PUSH_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                        (Constants.RANGER_ADMIN_USER + ":" + Constants.RANGER_ADMIN_PASSWORD).getBytes()))
                .PUT(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        return sendAsyncWithRetry(request, MAX_RETRIES);
    }

    private CompletableFuture<HttpResponse<String>> sendAsyncWithRetry(HttpRequest request, int retriesLeft) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                if (retriesLeft == 0) {
                    throw new RuntimeException("Failed to send request after " + MAX_RETRIES + " retries", e);
                } else {
                    CompletableFuture<HttpResponse<String>> future = new CompletableFuture<>();
                    scheduler.schedule(() -> sendAsyncWithRetry(request, retriesLeft - 1)
                            .whenComplete((response, ex) -> {
                                if (ex != null) {
                                    future.completeExceptionally(ex);
                                } else {
                                    future.complete(response);
                                }
                            }), RETRY_DELAY, TimeUnit.SECONDS);
                    return future.join();
                }
            }
        }, executor);
    }

    public void close() {
        executor.shutdown();
        scheduler.shutdown();
    }

}
