package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SimpleTextCorpusReader implements RepositoryReader {

	File input;
	BufferedReader br;
	String id, doc;
	String separator;
	StringBuilder sb;

	public SimpleTextCorpusReader(File input, String separator) throws IOException {
		this.input = input;
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

	@Override
	public String getID() {
		return id;
	}

	@Override
	public String getDoc() {
		return doc;
	}

	@Override
	public void close() throws IOException {
		br.close();
	}

	@Override
	public String getStats() {
		return "read from: " + input;
	}

	@Override
	public String getDate() {
		return "unkown";
	}

	@Override
	public String getVersion() {
		return "unkown";
	}

	@Override
	public Set<String> getFiles() {
		return new HashSet<String>();
	}
}