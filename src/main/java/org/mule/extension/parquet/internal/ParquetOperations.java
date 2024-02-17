package org.mule.extension.parquet.internal;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.mule.extension.parquet.internal.int96.ParquetTimestampUtils;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.FILE;

public class ParquetOperations {
    private final Logger LOGGER = LoggerFactory.getLogger(ParquetOperations.class);

    @MediaType(value = MediaType.APPLICATION_JSON, strict = false)
    @DisplayName("Read Parquet - File")
    public String readParquet(@DisplayName("Parquet File Location") @org.mule.runtime.extension.api.annotation.param.display.Path(type = FILE, location = EXTERNAL) String parquetFilePath) {

        ParquetReader<SimpleRecord> reader = null;
        JsonArray array = new JsonArray();
        JsonParser parser = new JsonParser();
        String item = null;

        try {
            reader = ParquetReader.builder(new SimpleReadSupport(), new Path(parquetFilePath)).build();
            ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), new Path(parquetFilePath));

            JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());

            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                item = formatter.formatRecord(value);
                JsonObject jsonObject = (JsonObject) parser.parse(item);
                array.add(jsonObject);
            }

        } catch (IllegalArgumentException e) {
            LOGGER.error(e.getMessage());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        return array.toString();
    }

    @MediaType(value = MediaType.APPLICATION_JSON, strict = false)
    @DisplayName("Read Parquet - Stream")
    public String readParquetStream(InputStream body) {

        String item = null;
        List<String> records = new ArrayList<>();

        try {
            ParquetBufferedReader inputFile = new ParquetBufferedReader(item, body);

            Configuration conf = new Configuration();
            conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);

            ParquetReader<GenericRecord> r = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .disableCompatibility()
                    .withConf(conf)
                    .build();
            GenericRecord record;

            while ((record = r.read()) != null) {
                String jsonRecord = deserialize(record.getSchema(), toByteArray(record.getSchema(), record)).toString();
                jsonRecord = ParquetTimestampUtils.convertInt96(jsonRecord);
                records.add(jsonRecord);
            }

        } catch (IllegalArgumentException e) {
            LOGGER.error(e.getMessage());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        return records.toString();
    }

    @MediaType(value = MediaType.ANY, strict = false)
    @DisplayName("Batch by Batch - Stream")
    public String readAndSendToHttp(@Connection ParquetConnection connection, @Config ParquetConfiguration config, long fetchSize, InputStream body) {
        List<String> recordList = new ArrayList<>();
        long total = 0;
        try {
            ParquetBufferedReader inputFile = new ParquetBufferedReader(null, body);
            Configuration conf = new Configuration();
            conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);

            ParquetReader<GenericRecord> r = AvroParquetReader.<GenericRecord>builder(inputFile).disableCompatibility().withConf(conf).build();
            GenericRecord record = null;

            long count = 0;
            while ((record = r.read()) != null) {
                if (count < fetchSize) {
                    String jsonRecord = deserialize(record.getSchema(), toByteArray(record.getSchema(), record)).toString();
                    recordList.add(jsonRecord);
                    count = count + 1;
                } else {
                    sendDataToHttp(connection, config, recordList.toString());
                    count = 0;
                    recordList = new ArrayList<>();
                }
                total = total + 1;
            }
            if (!recordList.isEmpty()) {
                sendDataToHttp(connection, config, recordList.toString());
                count = 0;
                recordList = new ArrayList<>();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        System.out.println("Total: " + total);
        LOGGER.info("Total recordList processed: " + total);
        return "Total recordList processed: " + total;
    }

    private void sendDataToHttp(ParquetConnection connection, ParquetConfiguration configuration, String postData) {
        connection.callHttp(configuration.getTargetUrl(), postData, configuration.getTimeout());
    }

    private GenericRecord deserialize(Schema schema, byte[] data) throws IOException {
        GenericData.get().addLogicalTypeConversion(new TimestampMillisConversion());
        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, GenericData.get());
        return reader.read(null, decoder);
    }

    private byte[] toByteArray(Schema schema, GenericRecord genericRecord) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.getData().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

    public static class TimestampMillisConversion extends Conversion<String> {
        public TimestampMillisConversion() {
        }

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        public String fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
            return Instant.ofEpochMilli(millisFromEpoch).toString();
        }

        public Long toLong(String timestamp, Schema schema, LogicalType type) {
            return new Long(timestamp);
        }
    }
}