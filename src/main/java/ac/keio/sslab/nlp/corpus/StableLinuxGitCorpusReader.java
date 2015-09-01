package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.eclipse.jgit.revwalk.RevTag;

import ac.keio.sslab.utils.SimpleGitReader;

public class StableLinuxGitCorpusReader implements GitCorpusReader {

	TreeMap<Date, GitCorpusReaderArguments> arguments;
	List<String> lts;
	String fileStr;
	GitCorpusReader reader;

	private class GitCorpusReaderArguments {
		public File input;
		public RevTag sinceTag, untilTag;
		public String fileStr;

		public GitCorpusReaderArguments(File input, RevTag sinceTag, RevTag untilTag, String fileStr) {
			this.input = input;
			this.sinceTag = sinceTag;
			this.untilTag = untilTag;
			this.fileStr = fileStr;
		}
	}

	public StableLinuxGitCorpusReader(File input, String fileStr) throws Exception {
		arguments = new TreeMap<>();
		this.fileStr = fileStr;
		SimpleGitReader g = new SimpleGitReader(input);
		TreeMap<Date, RevTag> tags = g.getTagAndDates();
		Map<RevTag, RevTag> r = new HashMap<>();
		for (Entry<Date, RevTag> e: tags.entrySet()) {
			String tag = e.getValue().getName();
			if (tag.lastIndexOf("rc") != -1 || tag.lastIndexOf('-') != -1) { //ignore rc versions
				continue;
			} else if (!tag.startsWith("v")) {
				continue;
			}
			String majorStr = tag.substring(0, tag.lastIndexOf('.'));

			if (majorStr.equals("v4") || majorStr.equals("v3") || majorStr.equals("v2.6")) {
				r.put(g.getTag(tag), e.getValue());
			} else if (g.getTagDate(g.getTag(majorStr)).before(g.getTagDate(e.getValue()))) {
				r.put(g.getTag(majorStr), e.getValue());
			}
		}
		g.close();

		lts = new ArrayList<>();
		for (Entry<RevTag, RevTag> e2: r.entrySet()) {
			arguments.put(g.getTagDate(e2.getKey()), new GitCorpusReaderArguments(input, e2.getKey(), e2.getValue(), fileStr));
		}

		for (Entry<Date, GitCorpusReaderArguments> e2: arguments.entrySet()) {
			GitCorpusReaderArguments arg = e2.getValue();
			System.out.println("Extract: " + arg.sinceTag + " -- " + arg.untilTag);
			lts.add(arg.sinceTag.getName() + " - " + arg.untilTag.getName() + "(" + g.getTagDateString(arg.sinceTag) + " - " + g.getTagDateString(arg.untilTag) + ")");
		}

		GitCorpusReaderArguments arg = arguments.firstEntry().getValue();
		arguments.remove(arguments.firstKey());
		reader = new GitLogCorpusReader(arg.input, arg.sinceTag, arg.untilTag, arg.fileStr);
	}

	@Override
	public boolean seekNext() throws Exception {
		boolean got = reader.seekNext();
		while (!got) {
			if (arguments.isEmpty())
				return false;
			reader.close();
			GitCorpusReaderArguments arg = arguments.firstEntry().getValue();
			arguments.remove(arguments.firstKey());
			try {
				reader = new GitLogCorpusReader(arg.input, arg.sinceTag, arg.untilTag, arg.fileStr);
			} catch (Exception e) {
				throw new IOException(e);
			}
			got = reader.seekNext();
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
		for (String s: lts) {
			sb.append(s).append('\n');
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
