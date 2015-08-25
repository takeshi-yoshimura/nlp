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
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
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

	public String getSubject(String sha) throws IOException {
		return getCommit(sha).getShortMessage();
	}

	public String getCommitDateString(String sha) throws IOException {
		return new SimpleDateFormat("yyyy-MM-dd").format(getCommitDate(sha));
	}

	public Date getCommitDate(String sha) throws IOException {
		return new Date(getCommit(sha).getCommitTime() * 1000);
	}

	public String getLatestTag(String sha) throws IOException {
		Date d = getCommitDate(sha);
		Date latest = tags.lowerKey(d);
		if (latest == null) {
			return "unkown";
		}
		return tags.get(latest);
	}

	public Set<String> getFiles(String sha) throws IOException, GitAPIException {
		Set<String> files = new HashSet<String>();
		for (DiffEntry entry : getDiffs(sha)) {
			files.add(entry.getNewPath());
		}
		return files;
	}

	public List<DiffEntry> getDiffs(String sha) throws IOException,
			GitAPIException {
		ObjectReader reader = repo.newObjectReader();
		CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
		oldTreeIter.reset(reader, getCommit(sha + "^"));
		CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
		newTreeIter.reset(reader, getCommit(sha));
		return git.diff().setNewTree(newTreeIter).setOldTree(oldTreeIter).call();
	}
}
