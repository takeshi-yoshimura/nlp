package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.jgit.revwalk.RevCommit;

import ac.keio.sslab.utils.SimpleGitReader;

public class ShaFileGitCorpusReader implements GitCorpusReader {

	SimpleGitReader git;
	Set<String> shas;
	Iterator<String> sha_iterator;
	String sha, doc, date, ver;
	Set<String> files;
	File input;

	public ShaFileGitCorpusReader(File input, File gitDir) throws Exception {
		this.input = input;
		git = new SimpleGitReader(gitDir);

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
	public boolean seekNext() throws Exception {
		if (!sha_iterator.hasNext())
			return false;
		RevCommit rev = git.getCommit(sha_iterator.next());
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
		return new String("Extracted: " + input.getAbsolutePath());
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
