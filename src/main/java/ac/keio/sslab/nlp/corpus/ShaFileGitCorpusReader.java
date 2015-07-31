package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class ShaFileGitCorpusReader implements GitCorpusReader {

	Repository repo;
	Git git;
	Set<String> shas;
	Iterator<String> sha_iterator;
	RevWalk walk;
	String sha, doc;
	File input;

	public ShaFileGitCorpusReader(File input, File gitDir) throws Exception {
		this.input = input;
		repo = new FileRepositoryBuilder().findGitDir(gitDir).build();
		git = new Git(repo);
		walk = new RevWalk(repo);

		shas = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(input));
		String str = br.readLine();
		while(str != null) {
			shas.add(str);
			str = br.readLine();
		}
		br.close();

		sha_iterator = shas.iterator();
	}

	@Override
	public boolean seekNext() throws IOException {
		if (!sha_iterator.hasNext())
			return false;
		RevCommit rev = walk.parseCommit(repo.resolve(sha_iterator.next()));
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
		walk.close();
		git.close();
		repo.close();
	}

	@Override
	public String getStats() {
		return new String("Extracted: " + input.getAbsolutePath());
	}

}
