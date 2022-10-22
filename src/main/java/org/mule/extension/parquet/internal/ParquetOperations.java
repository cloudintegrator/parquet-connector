package org.mule.extension.parquet.internal;

import static org.mule.extension.parquet.internal.io.OutputFile.nioPathToOutputFile;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;
import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.FILE;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.nio.file.Paths;

import javax.annotation.Nonnull;

import static org.mule.runtime.extension.api.annotation.param.Optional.PAYLOAD;

public class ParquetOperations {

	@MediaType(value = ANY, strict = false)
	@DisplayName("Write Avro to Parquet - Stream")
	public InputStream writeAvroToParquetStream(@Optional(defaultValue = PAYLOAD) InputStream body,
			@Optional(defaultValue = "UNCOMPRESSED") @DisplayName("Compression Codec") CompressionCodecName codec)
			throws IOException {

		GenericDatumReader<Object> greader = new GenericDatumReader<Object>();
		DataFileStream dataStreamReader = new DataFileStream(body, greader);
		Schema avroSchema = dataStreamReader.getSchema();

		ParquetBufferedWriter outputFile = new ParquetBufferedWriter();

		ParquetWriter<Object> writer = AvroParquetWriter.builder(outputFile)
				.withRowGroupSize(256 * 1024 * 1024)
				.withPageSize(1024 * 1024)
				.withSchema(avroSchema)
				.withConf(new Configuration()).withCompressionCodec(codec).withValidation(false)
				.withDictionaryEncoding(false)
				.build();

		GenericRecord avroRecord = null;
		while (dataStreamReader.hasNext()) {
			avroRecord = (GenericRecord) dataStreamReader.next();
			writer.write((Record) avroRecord);
		}
		writer.close();
		dataStreamReader.close();

		return new ByteArrayInputStream(outputFile.toArray());
	}

	@MediaType(value = ANY, strict = false)
	@DisplayName("Write Avro to Parquet - File")
	public InputStream writeAvroToParquet(@Optional(defaultValue = PAYLOAD) InputStream body,
			@DisplayName("File Output Location") @org.mule.runtime.extension.api.annotation.param.display.Path(type = FILE, location = EXTERNAL) String parquetFilePath,
			@Optional(defaultValue = "UNCOMPRESSED") @DisplayName("Compression Codec") CompressionCodecName codec)
			throws IOException {

		GenericDatumReader<Object> greader = new GenericDatumReader<Object>();
		DataFileStream dataStreamReader = new DataFileStream(body, greader);

		// convert Avro schema to Parquet schema
		Schema avroSchema = dataStreamReader.getSchema();
		MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
		AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);

		java.nio.file.Path outputPath = Paths.get(parquetFilePath);

		final ParquetWriter<GenericData.Record> writer = createParquetWriterInstance(avroSchema, outputPath, codec);

		GenericRecord avroRecord = null;
		while (dataStreamReader.hasNext()) {
			avroRecord = (GenericRecord) dataStreamReader.next();
			writer.write((Record) avroRecord);
			// System.out.print(avroRecord.toString());
		}
		writer.close();
		dataStreamReader.close();

		return body;
	}

	private static ParquetWriter<GenericData.Record> createParquetWriterInstance(@Nonnull final Schema schema,
			@Nonnull final java.nio.file.Path fileToWrite, CompressionCodecName codec) throws IOException {
		return AvroParquetWriter.<GenericData.Record>builder(nioPathToOutputFile(fileToWrite))
				.withRowGroupSize(256 * 1024 * 1024).withPageSize(1024 * 1024).withSchema(schema)
				.withConf(new Configuration()).withCompressionCodec(codec).withValidation(false)
				.withDictionaryEncoding(false).build();
	}

	@MediaType(value = MediaType.APPLICATION_JSON, strict = false)
	@DisplayName("Read Parquet - File")
	public String readParquet(
			@DisplayName("Parquet File Location") @org.mule.runtime.extension.api.annotation.param.display.Path(type = FILE, location = EXTERNAL) String parquetFilePath) {

		ParquetReader<SimpleRecord> reader = null;
		JsonArray array = new JsonArray();
		JsonParser parser = new JsonParser();
		String item = null;

		try {
			reader = ParquetReader.builder(new SimpleReadSupport(), new Path(parquetFilePath)).build();
			ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), new Path(parquetFilePath));

			JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter
					.fromSchema(metadata.getFileMetaData().getSchema());

			for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
				item = formatter.formatRecord(value);
				JsonObject jsonObject = (JsonObject) parser.parse(item);
				array.add(jsonObject);
			}

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return array.toString();
	}

	@MediaType(value = MediaType.APPLICATION_JSON, strict = false)
	@DisplayName("Read Parquet - Stream")
	public String readParquetStream(@Optional(defaultValue = PAYLOAD) InputStream body){

		String item = null;
		OutputStream outputStream = new ByteArrayOutputStream(1024); 

		try {
			ParquetBufferedReader inputFile = new ParquetBufferedReader(item, body);

			ParquetReader<GenericRecord> r = AvroParquetReader.<GenericRecord>builder(inputFile)
                                                                             .disableCompatibility()
                                                                             .build();
			GenericRecord record;
			
			while ((record = r.read()) != null) {

				DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
				JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);

				writer.write(record, encoder);
				encoder.flush();
			}
			
		}  catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputStream.toString();
	}
}