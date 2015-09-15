package ac.keio.sslab.analytics;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import ac.keio.sslab.utils.StringSplitTextReader;

public class CorpusIDIndexReader {

	StringSplitTextReader r;
	int key;
	int value;

	public CorpusIDIndexReader(File bottomupDir) throws IOException {
		r = new StringSplitTextReader(new File(bottomupDir, "corpusIDIndex.csv"), ",");
	}

	public boolean seekNext() throws IOException {
		if (!r.seekNext()) {
			return false;
		}
		key = Integer.parseInt(r.next());
		if (!r.seekNext()) {
			throw new IOException("Broken");
		}
		value = Integer.parseInt(r.next());
		return true;
	}

	public int key() {
		return key;
	}

	public int val() {
		return value;
	}

	public void close() throws IOException {
		r.close();
	}

	public Map<Integer, Integer> all() throws IOException {
		Map<Integer, Integer> all = new HashMap<>();
		while (seekNext()) {
			all.put(key(), val());
		}
		close();
		return all;
	}
}
