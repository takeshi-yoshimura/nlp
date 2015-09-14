package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ac.keio.sslab.utils.StringSplitTextReader;

public class PatchEntryReader {

	StringSplitTextReader r;
	String patchID;
	String date, ver;
	Set<String> files;

	public PatchEntryReader(File corpusDir) throws IOException {
		r = new StringSplitTextReader(new File(corpusDir, "commits.txt"), ",");
	}

	public boolean seekNext() throws IOException {
		if (!r.seekNext()) {
			return false;
		}
		patchID = r.next();
		date = r.next();
		ver = r.next();
		files = new HashSet<>();
		while (!r.isEndOfLine()) {
			if (!r.seekNext()) {
				break;
			}
			files.add(r.next());
		}
		return true;
	}

	public String patchID() {
		return patchID;
	}

	public String date() {
		return date;
	}

	public String version() {
		return ver;
	}

	public Set<String> files() {
		return files;
	}

	public void close() throws IOException {
		r.close();
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

	public Map<String, PatchEntry> all() throws IOException {
		Map<String, PatchEntry> patchEntries = new HashMap<>();
		while (seekNext()) {
			patchEntries.put(patchID(), new PatchEntry(date(), version(), files()));
		}
		close();
		return patchEntries;
	}
}
