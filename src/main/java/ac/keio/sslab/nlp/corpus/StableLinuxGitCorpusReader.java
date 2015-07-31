package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class StableLinuxGitCorpusReader implements GitCorpusReader {

	Repository repo;
	Git git;
	List<GitCorpusReader> readers;
	int readerIndex;
	Map<String, String> lts;
	String fileStr;

	public StableLinuxGitCorpusReader(File input, String fileStr) throws Exception {
		this.fileStr = fileStr;
		repo = new FileRepositoryBuilder().findGitDir(input).build();
		git = new Git(repo);
		RevWalk walk = new RevWalk(repo);
		Map<String, String> rangeMap = new HashMap<String, String>();
		for (Entry<String, Ref> e: repo.getTags().entrySet()) {
			String tag = e.getKey();
			if (tag.lastIndexOf("rc") != -1 || tag.lastIndexOf('-') != -1) { //ignore rc versions
				continue;
			} else if (!tag.startsWith("v")) {
				continue;
			}
			String majorStr = tag.substring(0, tag.lastIndexOf('.'));
			RevObject c = walk.peel(walk.parseAny(repo.resolve(tag)));
			if (!(c instanceof RevCommit)) {
				continue;
			}
			int time =  walk.parseCommit(repo.resolve(tag)).getCommitTime();
			if (!rangeMap.containsKey(majorStr) && !tag.equals("v3") && !tag.equals("v2.6")) {
				rangeMap.put(majorStr, tag);
			} else {
				String last = rangeMap.get(majorStr);
				int prevUntil = walk.parseCommit(repo.resolve(last)).getCommitTime();
				if (prevUntil < time) {
					rangeMap.put(majorStr, tag);
				}
			}
		}

		lts = new HashMap<String, String>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		for (Entry<String, String> e2: rangeMap.entrySet()) {
			System.out.println("Extract: " + e2.getKey() + " -- " + e2.getValue());
			ObjectId sinceRef = repo.resolve(e2.getKey());
			ObjectId untilRef = repo.resolve(e2.getValue());
			if (sinceRef == null || untilRef == null) {
				continue;
			}
			long since = walk.parseCommit(sinceRef).getCommitTime();
			long until = walk.parseCommit(untilRef).getCommitTime();
			if ((until - since) / 3600 / 24 > 365) {
				lts.put(e2.getKey() + " - " + e2.getValue(), sdf.format(new Date(since * 1000)) + " - " + sdf.format(new Date(until * 1000)));
				readers.add(new GitLogCorpusReader(input, e2.getKey(), e2.getValue(), fileStr));
			}
		}
		walk.close();
		readerIndex = 0;
	}
	@Override
	public boolean seekNext() throws IOException {
		GitCorpusReader reader = readers.get(readerIndex);
		boolean got = reader.seekNext();
		while (got) {
			if (++readerIndex == readers.size())
				return false;
			got = readers.get(readerIndex).seekNext();
		}
		return true;
	}

	@Override
	public String getSha() {
		return readers.get(readerIndex).getSha();
	}

	@Override
	public String getDoc() {
		return readers.get(readerIndex).getDoc();
	}

	@Override
	public void close() throws IOException {
		for (GitCorpusReader reader: readers) {
			reader.close();
		}
		git.close();
		repo.close();
	}
	@Override
	public String getStats() {
		StringBuilder sb = new StringBuilder();
		sb.append("Extracted versions: ");
		for (Entry<String, String> e3: lts.entrySet()) {
			sb.append(e3.getKey()).append('(').append(e3.getValue()).append(')').append(',');
		}
		sb.setLength(sb.length() - 1);
		sb.append('\n').append("directory: ").append(fileStr);
		return sb.toString();
	}
}
