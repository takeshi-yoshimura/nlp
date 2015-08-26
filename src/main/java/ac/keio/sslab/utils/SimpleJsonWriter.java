package ac.keio.sslab.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

public class SimpleJsonWriter {

	JsonGenerator json;

	public SimpleJsonWriter(Writer writer) throws IOException {
		ObjectMapper mapper = new ObjectMapper().configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		json = mapper.getJsonFactory().createJsonGenerator(writer);
		json.setPrettyPrinter(new DefaultPrettyPrinter());
		json.writeStartObject();
	}

	public SimpleJsonWriter(File output) throws IOException {
		this(new OutputStreamWriter(new FileOutputStream(output), "UTF-8"));
	}

	public void writeStringDoubleMap(String fieldName, Map<String, Double> map) throws IOException {
		json.writeFieldName(fieldName);
		json.writeStartObject();
		for (Entry<String, Double> e2: map.entrySet()) {
			json.writeNumberField(e2.getKey(), e2.getValue());
		}
		json.writeEndObject();
	}

	public void writeStringCollection(String fieldName, Collection<String> col) throws IOException {
		json.writeFieldName(fieldName);
		json.writeStartArray();
		for (String sha: col) {
			json.writeString(sha);
		}
		json.writeEndArray();
	}

	public void writeStringListStringCollection(String fieldName, Map<String, List<String>> map) throws IOException {
		json.writeFieldName(fieldName);
		json.writeStartObject();
		for (Entry<String, List<String>> p: map.entrySet()) {
			writeStringCollection(p.getKey(), p.getValue());
		}
		json.writeEndObject();
	}

	public void writeStartObject(String fieldName) throws IOException {
		json.writeFieldName(fieldName);
		json.writeStartObject();
	}

	public void writeEndObject() throws IOException {
		json.writeEndObject();
	}

	public void writeStringField(String fieldName, String value) throws IOException {
		json.writeStringField(fieldName, value);
	}

	public void writeNumberField(String fieldName, int number) throws IOException {
		json.writeNumberField(fieldName, number);
	}

	public void writeNumberField(String fieldName, double number) throws IOException {
		json.writeNumberField(fieldName, number);
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	public void writeDateField(String fieldName, Date date) throws IOException {
		json.writeStringField(fieldName, sdf.format(date));
	}

	public void writeDateCollection(String fieldName, List<Date> dates) throws Exception {
		json.writeFieldName(fieldName);
		json.writeStartArray();
		for (Date date: dates) {
			json.writeString(sdf.format(date));
		}
		json.writeEndArray();
	}

	public void close() throws IOException {
		json.writeEndObject();
		json.close();
	}
}
