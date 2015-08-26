package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import ac.keio.sslab.utils.SimpleGitReader;

public class StableLinuxGitCorpusReader implements GitCorpusReader {

	List<GitCorpusReaderArguments> arguments;
	int readerIndex;
	Map<String, String> lts;
	String fileStr;
	GitCorpusReader reader;

	private class GitCorpusReaderArguments {
		public File input;
		public String sinceStr, untilStr, fileStr;

		public GitCorpusReaderArguments(File input, String sinceStr, String untilStr, String fileStr) {
			this.input = input;
			this.sinceStr = sinceStr;
			this.untilStr = untilStr;
			this.fileStr = fileStr;
		}
	}

	public StableLinuxGitCorpusReader(File input, String fileStr) throws Exception {
		arguments = new ArrayList<GitCorpusReaderArguments>();
		this.fileStr = fileStr;
		SimpleGitReader g = new SimpleGitReader(input);
		TreeMap<Date, String> tags = g.getTagAndDates();
		Map<String, String> r = new HashMap<String, String>();
		for (Entry<Date, String> e: tags.entrySet()) {
			String tag = e.getValue();
			if (tag.lastIndexOf("rc") != -1 || tag.lastIndexOf('-') != -1) { //ignore rc versions
				continue;
			} else if (!tag.startsWith("v")) {
				continue;
			}
			String majorStr = tag.substring(0, tag.lastIndexOf('.'));

			if (majorStr.equals("v4") || majorStr.equals("v3") || majorStr.equals("v2.6")) {
				r.put(tag, tag);
			} else {
				r.put(majorStr, tag);
			}
		}
		g.close();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		lts = new HashMap<String, String>();
		for (Entry<String, String> e2: r.entrySet()) {
			System.out.println("Extract: " + e2.getKey() + " -- " + e2.getValue());
			Date sinceD = null, untilD = null;
			for (Entry<Date, String> e: tags.entrySet()) {
				if (e.getValue().equals(e2.getKey())) {
					sinceD = e.getKey();
				}
				if (e.getValue().equals(e2.getValue())) {
					untilD = e.getKey();
				}
			}
			lts.put(e2.getKey() + " - " + e2.getValue(), sdf.format(sinceD) + " - " + sdf.format(untilD));
			arguments.add(new GitCorpusReaderArguments(input, e2.getKey(), e2.getValue(), fileStr));
		}
		readerIndex = 0;
		GitCorpusReaderArguments arg = arguments.get(0);
		reader = new GitLogCorpusReader(arg.input, arg.sinceStr, arg.untilStr, arg.fileStr);
	}

	public boolean seekNextStable() throws Exception {
		boolean result = false;
		do {
			result = reader.seekNext();
		} while (result && reader.getVersion().contains("rc"));

		return result;
	}

	@Override
	public boolean seekNext() throws Exception {
		boolean got = seekNextStable();
		while (!got) {
			if (++readerIndex == arguments.size())
				return false;
			reader.close();
			GitCorpusReaderArguments arg = arguments.get(readerIndex);
			try {
				reader = new GitLogCorpusReader(arg.input, arg.sinceStr, arg.untilStr, arg.fileStr);
			} catch (Exception e) {
				throw new IOException(e);
			}
			got = seekNextStable();
		}
		return true;
	}

	@Override
	public String getSha() {
		return reader.getSha();
	}

	@Override
	public String getDoc() {
		return reader.getDoc();
	}

	@Override
	public void close() throws Exception {
		reader.close();
	}

	@Override
	public String getStats() {
		StringBuilder sb = new StringBuilder();
		sb.append("Extracted versions:\n");
		for (Entry<String, String> e3: lts.entrySet()) {
			sb.append(e3.getKey()).append('(').append(e3.getValue()).append(')').append('\n');
		}
		sb.append("directory: ").append(fileStr);
		return sb.toString();
	}

	@Override
	public String getDate() {
		return reader.getDate();
	}

	@Override
	public String getVersion() {
		return reader.getVersion();
	}

	@Override
	public Set<String> getFiles() {
		return reader.getFiles();
	}
}
