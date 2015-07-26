package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class GitLogCorpusReader implements GitCorpusReader {
	Repository repo;
	Git git;
	Iterator<RevCommit> logs;
	String sha, doc;

	public GitLogCorpusReader(File input, String sinceStr, String untilStr, String fileStr) throws Exception {
		repo = new FileRepositoryBuilder().findGitDir(input).build();
		git = new Git(repo);
		RevWalk walk = new RevWalk(repo);
		RevCommit untilRef = walk.parseCommit(repo.resolve(untilStr));
		if (!sinceStr.equals("")) {
			RevCommit sinceRef = walk.parseCommit(repo.resolve(sinceStr));
			if (fileStr == null) {
				logs = git.log().addRange(sinceRef, untilRef).call().iterator();
			} else {
				logs = git.log().addRange(sinceRef, untilRef).addPath(fileStr).call().iterator();
			}
		} else {
			if (fileStr == null) {
				logs = git.log().add(untilRef).call().iterator();
			} else {
				logs = git.log().add(untilRef).addPath(fileStr).call().iterator();
			}
		}
		walk.close();
	}

	@Override
	public boolean seekNext() throws IOException {
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
		repo.close();
	}
}
