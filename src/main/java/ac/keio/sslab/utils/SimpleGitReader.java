package ac.keio.sslab.utils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

public class SimpleGitReader {

	Repository repo;
	Git git;
	RevWalk walk;
	TreeMap<Date, String> tags;

	public SimpleGitReader(File gitDir) throws IOException {
		repo = new FileRepositoryBuilder().findGitDir(gitDir).build();
		git = new Git(repo);
		walk = new RevWalk(repo);
		tags = getTagAndDates();
	}

	public TreeMap<Date, String> getTagAndDates() throws IOException {
		TreeMap<Date, String> vers = new TreeMap<Date, String>();
		for (Entry<String, Ref> e: repo.getTags().entrySet()) {
			String tag = e.getKey();
			RevObject c = walk.peel(walk.parseAny(repo.resolve(tag)));
			if (!(c instanceof RevCommit)) {
				continue;
			}
			vers.put(getCommitDate(tag), tag);
		}
		return vers;
	}

	public RevCommit getCommit(String sha) throws IOException {
		return walk.parseCommit(repo.resolve(sha));
	}

	public String getFullMessage(String sha) throws IOException {
		return getCommit(sha).getFullMessage();
	}

	public String getSubject(String sha) throws IOException {
		return getCommit(sha).getShortMessage();
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	public String getCommitDateString(String sha) throws IOException {
		return sdf.format(getCommitDate(sha));
	}

	public String getCommitDateString(RevCommit commit) throws IOException {
		return sdf.format(getCommitDate(commit));
	}

	public Date getCommitDate(String sha) throws IOException {
		return new Date(1000L * getCommit(sha).getCommitTime());
	}

	public Date getCommitDate(RevCommit commit) throws IOException {
		return new Date(1000L * commit.getCommitTime());
	}

	public String getLatestTag(String sha) throws IOException {
		Date d = getCommitDate(sha);
		if (tags.containsKey(d)) {
			return tags.get(d);
		}
		Date latest = tags.lowerKey(d);
		if (latest == null) {
			return "unkown";
		}
		return tags.get(latest);
	}

	public String getLatestTag(RevCommit commit) throws IOException {
		Date d = getCommitDate(commit);
		if (tags.containsKey(d)) {
			return tags.get(d);
		}
		Date latest = tags.lowerKey(d);
		if (latest == null) {
			return "unkown";
		}
		return tags.get(latest);
	}

	public Set<String> getFiles(String sha) throws Exception {
		Set<String> files = new HashSet<String>();
		for (DiffEntry entry : getDiffs(sha)) {
			files.add(entry.getNewPath());
		}
		return files;
	}

	public List<DiffEntry> getDiffs(String sha) throws Exception {
		return git.diff().setNewTree(getTreeIterator(sha)).setOldTree(getTreeIterator(sha + "^")).call();
	}

	// from jGit test code
	public AbstractTreeIterator getTreeIterator(String name) throws IOException {
		final ObjectId id = repo.resolve(name);
		if (id == null)
			throw new IllegalArgumentException(name);
		final CanonicalTreeParser p = new CanonicalTreeParser();
		try (ObjectReader or = repo.newObjectReader()) {
			p.reset(or, walk.parseTree(id));
			return p;
		}
	}

	public String showCommit(String sha) throws Exception {
		StringBuilder sb = new StringBuilder();
		RevCommit commit = getCommit(sha);
		sb.append("commit ").append(commit.getId().getName()).append('\n');
		sb.append("Author: ").append(commit.getAuthorIdent()).append('\n');
		sb.append("Date: ").append(new Date(1000L * commit.getCommitTime())).append('\n');
		sb.append(commit.getFullMessage()).append('\n');
		for (DiffEntry entry: getDiffs(sha)) {
			sb.append(entry).append('\n');
		}
		return sb.toString();
	}

	public LogCommand log() {
		return git.log();
	}

	public void close() {
		git.close();
		repo.close();
	}
}