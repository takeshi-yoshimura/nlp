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
	}

	public void readStartArray(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		if (json.nextToken() != JsonToken.START_ARRAY) {
			throw new IOException("Json parse error: should begin with '[' for Collection<?> at line " + json.getCurrentLocation().getLineNr());
		}
	}

	public Map<String, Double> readStringDoubleMap(String fieldName) throws IOException {
		readStartObject(fieldName);
		Map<String, Double> map = new HashMap<String, Double>();
		while (json.nextToken() != JsonToken.END_OBJECT) {
			String key = json.getCurrentName();
			json.nextToken();
			Double val = json.getDoubleValue();
			map.put(key, val);
		}
		return map;
	}

	public List<String> readStringCollection(String fieldName) throws IOException {
		readStartArray(fieldName);
		List<String> col = new ArrayList<String>();
		while (json.nextToken() != JsonToken.END_ARRAY) {
			String val = json.getCurrentName();
			col.add(val);
		}
		return col;
	}

	public Map<String, List<String>> readStringListStringCollection(String fieldName) throws IOException {
		readStartObject(fieldName);
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		while (json.nextToken() != JsonToken.END_OBJECT) {
			String key = json.getCurrentName();
			if (json.nextToken() != JsonToken.START_ARRAY) {
				throw new IOException("Parse error: should be here with '{' for Map<String, List<String>> at line " + json.getCurrentLocation().getLineNr());
			}
			List<String> col = new ArrayList<String>();
			while (json.nextToken() != JsonToken.END_ARRAY) {
				String val = json.getCurrentName();
				col.add(val);
			}
			map.put(key, col);
		}
		return map;
	}

	public String readStringField(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		return json.nextTextValue();
	}

	public void readEndObject() throws IOException {
		if (json.nextToken() != JsonToken.END_OBJECT) {
			throw new IOException("Parse error: should be '}' at line " + json.getCurrentLocation().getLineNr());
		}
	}

	public int readIntValue(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		json.nextToken();
		return json.getIntValue();
	}

	public double readDoubleValue(String fieldName) throws IOException {
		if (!fieldName.equals(json.getCurrentName())) {
			throw new IOException("Parse error: line " + json.getCurrentLocation().getLineNr() + " is not '" + fieldName + "'");
		}
		json.nextToken();
		return json.getDoubleValue();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	public Date readDateValue(String fieldName) throws Exception {
		return sdf.parse(readStringField(fieldName));
	}

	public void skipChildren() throws IOException {
		json.skipChildren();
	}

	public void close() throws IOException {
		json.close();
	}
}
