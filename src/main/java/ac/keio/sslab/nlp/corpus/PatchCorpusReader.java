package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// this class does not consider reading large corpora
public class PatchCorpusReader {

	File inputDir;

	public PatchCorpusReader(File inputDir) {
		this.inputDir = inputDir;
	}

	public Map<Integer, String> getOriginalDocuments() throws IOException {
		Map<Integer, String> documents = new HashMap<>();
		BufferedReader originalCorpusReader = new BufferedReader(new FileReader(new File(inputDir, "beforeStopWrodCorpus.txt")));
		String line = null;
		while ((line = originalCorpusReader.readLine()) != null) {
			int pointID = Integer.parseInt(line.substring(0, line.indexOf(' ')));
			String txt = line.substring(line.indexOf(' ') + 1);
			documents.put(pointID, txt);
		}
		originalCorpusReader.close();
		return documents;
	}

	public Map<Integer, List<String>> getIdIndex() throws IOException {
		Map<Integer, List<String>> idIndex = new HashMap<>();
		BufferedReader idIndexReader = new BufferedReader(new FileReader(new File(inputDir, "idIndex.txt")));
		String line = null;
		while ((line = idIndexReader.readLine()) != null) {
			if (line.isEmpty()) {
				continue;
			}
			List<String> patchIDs = new ArrayList<>();
			for (String patchID: line.substring(line.indexOf("\t\t") + 2).split(",")) {
				patchIDs.add(patchID);
			}
			idIndex.put(Integer.parseInt(line.substring(0, line.indexOf("\t\t"))), patchIDs);
		}
		idIndexReader.close();
		return idIndex;
	}

	public class PatchEntry {
		public String date, ver;
		public Set<String> files;
		public PatchEntry(String date, String ver, Set<String> files) {
			this.date = date;
			this.ver = ver;
			this.files = files;
		}
	}

	public Map<String, PatchEntry> getPatchEntries() throws IOException {
		Map<String, PatchEntry> patchEntries = new HashMap<>();
		BufferedReader patchesReader = new BufferedReader(new FileReader(new File(inputDir, "commits.txt")));
		String line = null;
		while ((line = patchesReader.readLine()) != null) {
			if (line.isEmpty()) {
				continue;
			}
			String [] split = line.split(",");
			int i = 0;
			String patchID = split[i++];
			String date = split[i++];
			String ver = split[i++];
			Set<String> files = new HashSet<String>();
			while (i < split.length) {
				files.add(split[i++]);
			}
			patchEntries.put(patchID, new PatchEntry(date, ver, files));
		}
		patchesReader.close();
		return patchEntries;
	}
}
