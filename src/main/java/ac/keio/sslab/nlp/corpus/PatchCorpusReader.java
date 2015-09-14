package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import ac.keio.sslab.nlp.corpus.PatchEntryReader.PatchEntry;

public class PatchCorpusReader {

	File inputDir;

	public PatchCorpusReader(File inputDir) {
		this.inputDir = inputDir;
	}

	public OriginalCorpusReader getOriginalCorpusReader() throws IOException {
		return new OriginalCorpusReader(new File(inputDir, "originalCorpus.txt"));
	}

	public BeforeStopWordCorpusReader getBeforeStopWordCorpusReader() throws IOException {
		return new BeforeStopWordCorpusReader(new File(inputDir, "beforeStopWrodCorpus.txt"));
	}

	public Map<Integer, List<String>> getBeforeStopWordDocuments() throws IOException {
		return getBeforeStopWordCorpusReader().all();
	}

	public IdIndexReader getIdIndexReader() throws IOException {
		return new IdIndexReader(new File(inputDir, "idIndex.txt"));
	}

	public Map<Integer, List<String>> getIdIndex() throws IOException {
		return getIdIndexReader().all();
	}

	public PatchEntryReader getPatchEntryReader() throws IOException {
		return new PatchEntryReader(new File(inputDir, "commits.txt"));
	}

	public Map<String, PatchEntry> getPatchEntries() throws IOException {
		return getPatchEntryReader().all();
	}
}
