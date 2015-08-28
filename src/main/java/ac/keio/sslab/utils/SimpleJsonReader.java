package ac.keio.sslab.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

public class SimpleJsonReader {

	JsonParser json;

	public SimpleJsonReader(Reader reader) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		json = mapper.getJsonFactory().createJsonParser(reader);
		json.nextToken();
		json.nextToken();
	}

	public SimpleJsonReader(File f) throws IOException {
		this(new InputStreamReader(new FileInputStream(f), "UTF-8"));
	}

	public String getCurrentFieldName() throws IOException {
		return json.getCurrentName();
	}

	public boolean isCurrentTokenEndObject() throws IOException {
		return json.getCurrentToken() == JsonToken.END_OBJECT;
	}

	public void readStartObject(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		if (json.nextToken() != JsonToken.START_OBJECT) {
			throw new IOException("Json parse error: should begin with '{' for Map<?, ?> at line " + json.getCurrentLocation().getLineNr());
		}
		json.nextToken();
	}

	public void readStartArray(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		if (json.nextToken() != JsonToken.START_ARRAY) {
			throw new IOException("Json parse error: should begin with '[' for Collection<?> at line " + json.getCurrentLocation().getLineNr());
		}
		json.nextToken();
	}

	public Map<String, Double> readStringDoubleMap(String fieldName) throws IOException {
		readStartObject(fieldName);
		Map<String, Double> map = new HashMap<String, Double>();
		do {
			String key = json.getCurrentName();
			json.nextToken();
			Double val = json.getDoubleValue();
			map.put(key, val);
		} while (json.nextToken() != JsonToken.END_OBJECT);
		json.nextToken();
		return map;
	}

	public List<String> readStringCollection(String fieldName) throws IOException {
		readStartArray(fieldName);
		List<String> col = new ArrayList<String>();
		do {
			col.add(json.getText());
		} while (json.nextToken() != JsonToken.END_ARRAY);
		json.nextToken();
		return col;
	}

	public Map<String, List<String>> readStringListStringCollection(String fieldName) throws IOException {
		readStartObject(fieldName);
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		do {
			String key = json.getCurrentName();
			if (json.nextToken() != JsonToken.START_ARRAY) {
				throw new IOException("Parse error: should be here with '{' for Map<String, List<String>> at line " + json.getCurrentLocation().getLineNr());
			}
			List<String> col = new ArrayList<String>();
			while (json.nextToken() != JsonToken.END_ARRAY) {
				col.add(json.getText());
			}
			map.put(key, col);
		} while (json.nextToken() != JsonToken.END_OBJECT);
		json.nextToken();
		return map;
	}

	public String readStringField(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		json.nextToken();
		String next = json.getText();
		json.nextToken();
		return next;
	}

	public void readEndObject() throws IOException {
		if (json.getCurrentToken() != JsonToken.END_OBJECT) {
			throw new IOException("Parse error: should be '}' at line " + json.getCurrentLocation().getLineNr());
		}
		json.nextToken();
	}

	public int readIntValue(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		json.nextToken();
		int next = json.getIntValue();
		json.nextToken();
		return next;
	}

	public double readDoubleValue(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		json.nextToken();
		double next = json.getDoubleValue();
		json.nextToken();
		return next;
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	public Date readDateValue(String fieldName) throws Exception {
		return sdf.parse(readStringField(fieldName));
	}

	public List<Date> readDateCollection(String fieldName) throws Exception {
		readStartArray(fieldName);
		List<Date> dates = new ArrayList<Date>();
		do {
			dates.add(sdf.parse(json.getText()));
		} while (json.nextToken() != JsonToken.END_ARRAY);
		json.nextToken();
		return dates;
	}

	public void skipChildren() throws IOException {
		json.skipChildren();
	}

	public void close() throws IOException {
		json.close();
	}
}
