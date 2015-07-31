package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class SimpleTextCorpusReader {

	BufferedReader br;
	String id, doc;
	String separator;
	StringBuilder sb;

	public SimpleTextCorpusReader(File input, String separator) throws IOException {
		br = new BufferedReader(new FileReader(input));
		this.separator = separator;
		String line = br.readLine();
		assert line.equals(separator);
		sb = new StringBuilder();
	}

	public boolean seekNext() throws IOException {
		String line;
		sb.setLength(0);
		while (true) {
			line = br.readLine();
			if (line == null)
				return false;
			else if (line.equals(separator))
				break;
			sb.append(line).append('\n');
		}
		id = sb.toString().trim();

		sb.setLength(0);
		while (true) {
			line = br.readLine();
			if (line == null)
				return false;
			else if (line.equals(separator))
				break;
			sb.append(line).append('\n');
		}
		doc = sb.toString().trim();

		return true;
	}

	public String getId() {
		return id;
	}

	public String getDoc() {
		return doc;
	}

	public void close() throws IOException {
		br.close();
	}
}