package org.mule.extension.parquet.internal;

import org.apache.http.entity.StringEntity;
import org.mule.runtime.http.api.HttpConstants;
import org.mule.runtime.http.api.HttpService;
import org.mule.runtime.http.api.client.HttpClient;
import org.mule.runtime.http.api.client.HttpClientConfiguration;
import org.mule.runtime.http.api.domain.entity.ByteArrayHttpEntity;
import org.mule.runtime.http.api.domain.message.request.HttpRequest;
import org.mule.runtime.http.api.domain.message.request.HttpRequestBuilder;
import org.mule.runtime.http.api.domain.message.response.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class ParquetConnection {
    private final Logger LOGGER = LoggerFactory.getLogger(ParquetConnection.class);
    private HttpClient httpClient;
    private HttpRequestBuilder httpRequestBuilder;

    public ParquetConnection(HttpService httpService) {
        HttpClientConfiguration.Builder builder = new HttpClientConfiguration.Builder();
        builder.setName("AnupamHttpConfig");
        this.httpClient = httpService.getClientFactory().create(builder.build());
        this.httpRequestBuilder = HttpRequest.builder();
        this.httpClient.start();
    }

    public void invalidate() {
        this.httpClient.stop();
    }

    public void callHttp(String url, String data, int timeout) {
        HttpResponse httpResponse = null;
        ByteArrayHttpEntity entity = new ByteArrayHttpEntity(data.getBytes(StandardCharsets.UTF_8));
        HttpRequest request = this.httpRequestBuilder
                .method(HttpConstants.Method.POST)
                .addHeader("Content-Type","application/json")
                .entity(entity)
                .uri(url)
                .build();

        try {
            httpResponse = this.httpClient.send(request, timeout, false, null);
            LOGGER.info("Sent to Http endpoint with status code " + httpResponse.getStatusCode());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
