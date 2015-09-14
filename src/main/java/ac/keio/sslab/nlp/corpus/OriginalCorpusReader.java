package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class OriginalCorpusReader {

	File f;

	public OriginalCorpusReader(File corpusDir) throws IOException {
		this.f = new File(corpusDir, "originalCorpus.txt");
	}

	public String get(String patchID) throws IOException {
		File file = new File(f, patchID);
		if (!file.exists()) {
			return null;
		}
		return FileUtils.readFileToString(file);
	}

	public String [] patchIDs() {
		return f.list();
	}

	public void close() {}
}
