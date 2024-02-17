package org.mule.extension.parquet.internal;

import org.mule.runtime.http.api.HttpConstants;
import org.mule.runtime.http.api.HttpService;
import org.mule.runtime.http.api.client.HttpClient;
import org.mule.runtime.http.api.client.HttpClientConfiguration;
import org.mule.runtime.http.api.domain.message.request.HttpRequest;
import org.mule.runtime.http.api.domain.message.request.HttpRequestBuilder;
import org.mule.runtime.http.api.domain.message.response.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

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

    public InputStream callHttp(String url) {
        HttpResponse httpResponse = null;
        HttpRequest request = this.httpRequestBuilder
                .method(HttpConstants.Method.GET)
                .uri(url)
                .build();
        try {
            httpResponse = this.httpClient.send(request, 1000, false, null);
            return httpResponse.getEntity().getContent();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }
}
