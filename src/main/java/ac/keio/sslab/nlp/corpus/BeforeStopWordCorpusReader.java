package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ac.keio.sslab.utils.StringSplitTextReader;

public class BeforeStopWordCorpusReader {

	StringSplitTextReader r;
	int key;
	List<String> value;

	public BeforeStopWordCorpusReader(File corpusDir) throws IOException {
		r = new StringSplitTextReader(new File(corpusDir, "beforeStopWordCorpus.txt"), " ");
	}

	public boolean seekNext() throws IOException {
		if (!r.seekNext()) {
			return false;
		}
		key = Integer.parseInt(r.next());
		value = new ArrayList<>();
		while (!r.isEndOfLine()) {
			if (!r.seekNext()) {
				break;
			}
			value.add(r.next());
		}
		return true;
	}

	public int key() {
		return key;
	}

	public List<String> val() {
		return value;
	}

	public void close() throws IOException {
		r.close();
	}

	public Map<Integer, List<String>> all() throws IOException {
		Map<Integer, List<String>> documents = new HashMap<>();
		while(seekNext()) {
			documents.put(key(), val());
		}
		close();
		return documents;
	}
}
