package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTag;

import ac.keio.sslab.utils.SimpleGitReader;

public class GitLogCorpusReader implements GitCorpusReader {
	SimpleGitReader git;
	Iterator<RevCommit> logs;
	String sha, doc, date, ver;
	Set<String> files;
	String sinceStr, untilStr, fileStr;

	public GitLogCorpusReader(File input, String sinceStr, String untilStr, String fileStr) throws Exception {
		this.sinceStr = sinceStr;
		this.untilStr = untilStr;
		this.fileStr = fileStr;
		git = new SimpleGitReader(input);
		RevCommit untilRef = git.getCommit(untilStr);
		RevCommit sinceRef = (sinceStr != null && !sinceStr.equals("")) ? git.getCommit(sinceStr): null;

		LogCommand cmd = git.log().add(untilRef);
		if (sinceRef != null) {
			cmd = git.log().addRange(sinceRef, untilRef);
		}
		if (fileStr != null) {
			cmd = cmd.addPath(fileStr);
		}
		logs = cmd.call().iterator();
	}

	public GitLogCorpusReader(File input, RevTag sinceTag, RevTag untilTag, String fileStr) throws Exception {
		git = new SimpleGitReader(input);
		this.sinceStr = sinceTag != null ? git.getTagDateString(sinceTag): null;
		this.untilStr = git.getTagDateString(untilTag);
		this.fileStr = fileStr;

		LogCommand cmd = git.log().add(untilTag);
		if (sinceTag != null) {
			cmd = git.log().addRange(sinceTag, untilTag);
		}
		if (fileStr != null) {
			cmd = cmd.addPath(fileStr);
		}
		logs = cmd.call().iterator();

	}

	@Override
	public boolean seekNext() throws Exception {
		if (!logs.hasNext()) {
			return false;
		}
		RevCommit rev = logs.next();
		while (rev.getParentCount() > 1) {
			if (!logs.hasNext())
				return false;
			rev = logs.next();
		}

		sha = rev.getId().getName();
		doc = rev.getFullMessage();
		date = git.getCommitDateString(rev);
		ver = git.getLatestTag(rev).getName();
		files = git.getFiles(sha);
		return true;
	}

	@Override
	public String getSha() {
		return sha;
	}

	@Override
	public String getDoc() {
		return doc;
	}

	@Override
	public void close() throws IOException {
		git.close();
	}

	@Override
	public String getStats() {
		StringBuilder sb = new StringBuilder();
		sb.append("Extracted: ").append(sinceStr).append(" - ").append(untilStr).append('\n');
		sb.append("directory: ").append(fileStr);
		return sb.toString();
	}

	@Override
	public String getDate() {
		return date;
	}

	@Override
	public String getVersion() {
		return ver;
	}

	@Override
	public Set<String> getFiles() {
		return files;
	}
}
