package org.mule.extension.parquet.internal;

import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.mule.runtime.api.connection.PoolingConnectionProvider;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.connection.CachedConnectionProvider;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

import org.mule.runtime.http.api.HttpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class ParquetConnectionProvider implements PoolingConnectionProvider<ParquetConnection> {

    private final Logger LOGGER = LoggerFactory.getLogger(ParquetConnectionProvider.class);

    @Inject
    private HttpService httpService;

    @Override
    public ParquetConnection connect() throws ConnectionException {
        return new ParquetConnection(this.httpService);
    }

    @Override
    public void disconnect(ParquetConnection connection) {
        try {
            connection.invalidate();
        } catch (Exception e) {
            LOGGER.error("Error while disconnecting []");
        }
    }

    @Override
    public ConnectionValidationResult validate(ParquetConnection connection) {
        return ConnectionValidationResult.success();
    }
}
