package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.revwalk.RevCommit;

import ac.keio.sslab.utils.SimpleGitReader;

public class GitLogCorpusReader implements GitCorpusReader {
	SimpleGitReader git;
	Iterator<RevCommit> logs;
	String sha, doc, date, ver;
	Set<String> files;
	String sinceStr, untilStr, fileStr;

	public GitLogCorpusReader(File input, String sinceStr, String untilStr, String fileStr, boolean isTag) throws Exception {
		git = new SimpleGitReader(input);
		if (!isTag) {
			this.sinceStr = sinceStr;
			this.untilStr = untilStr;
		} else {
			this.sinceStr = sinceStr != null ? git.getTagDateString(sinceStr): null;
			this.untilStr = git.getTagDateString(untilStr);
		}
		this.fileStr = fileStr;
		RevCommit until = git.getCommit(untilStr);
		RevCommit since = (sinceStr != null && !sinceStr.equals("")) ? git.getCommit(sinceStr): null;

		LogCommand cmd = git.log().add(until);
		if (since != null) {
			cmd = git.log().addRange(since, until);
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
		ver = git.descirbe(sha);
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
