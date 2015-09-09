package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;
import ac.keio.sslab.utils.mahout.SimpleLDAReader;

public class TopicTrendJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "topicTrend";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new LDAJob();
	}

	@Override
	public String getShortJobName() {
		return "tt";
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, getJobName());
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "calculate empirical trend of topics";
	}

	@Override
	public Options getOptions() {
		return null;
	}

	protected Map<Integer, String> getVersions(Repository repo) throws Exception {
		RevWalk walk = new RevWalk(repo);
		Map<Integer, String> ret = new TreeMap<Integer, String>();
		for (Entry<String, Ref> e: repo.getTags().entrySet()) {
			String tag = e.getKey();
			if (tag.lastIndexOf("rc") != -1 || tag.lastIndexOf('-') != -1) { //ignore rc versions
				continue;
			} else if (!tag.startsWith("v")) {
				continue;
			}
			RevObject c = walk.peel(walk.parseAny(repo.resolve(tag)));
			if (!(c instanceof RevCommit)) {
				continue;
			}
			ret.put(walk.parseCommit(repo.resolve(tag)).getCommitTime(), tag);
		}
		ret.put(0, "rc");
		walk.close();
		return ret;
	}

	protected Iterator<RevCommit> getIterator(Repository repo, Git git, String sinceStr, String untilStr, String fileStr) throws Exception {
		RevWalk walk = new RevWalk(repo);
		RevCommit untilRef = walk.parseCommit(repo.resolve(untilStr));
		Iterator<RevCommit> logs;
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
		return logs;
	}

	class GitMetaInfo {
		public int version;
		public Set<String> dirs;
		public GitMetaInfo(int version, Set<String> dirs) {
			this.version = version;
			this.dirs = dirs;
		}
	}

	@Override
	public void run(JobManager mgr) throws Exception {
		NLPConf conf = NLPConf.getInstance();
		File gitFile = new File(mgr.getArgStr("g"));
		File outputFile = mgr.getLocalOutputDir();
		outputFile.mkdirs();

		LDAHDFSFiles hdfs = new LDAHDFSFiles(mgr.getParentJobManager().getHDFSOutputDir());
		//get <sha, <ver, dirs>> and num(doc|ver), num(doc|dir) at first
		Repository repo = new FileRepositoryBuilder().findGitDir(gitFile).build();
		Map<Integer, String> vers = getVersions(repo); // <version time, version name>
		Map<Integer, Integer> verDocs = new TreeMap<Integer, Integer>(); //<version time, num(doc)>
		Map<String, Integer> dirDocs = new HashMap<String, Integer>(); //<dir, num(doc)>
		Map<Integer, GitMetaInfo> shas = getGitMetaInfo(hdfs.docIndexPath, conf.hdfs, repo, vers, verDocs, dirDocs); //<sha ID, version>

		System.out.println("Load topic");
		Map<Integer, String> topicNames = SimpleLDAReader.getTopicTerm(conf.hdfs, hdfs.dictionaryPath, hdfs.topicPath);

		System.out.println("Load p(topic|document) and calculate p(topic|ver), p(topic|dir)");
		Map<Integer, Map<Integer, Double>> pTopicVer = new HashMap<Integer, Map<Integer, Double>>(); //<topicID, <ver, p(topic|ver)>>
		Map<Integer, Map<String, Double>> pTopicDir = new HashMap<Integer, Map<String, Double>>(); //<topicID, <dir, p(topic|dir)>>
		SequenceDirectoryReader<Integer, Vector> docReader = new SequenceDirectoryReader<>(hdfs.documentPath, conf.hdfs, Integer.class, Vector.class);
		while (docReader.seekNext()) {
			int sha = docReader.key();
			if (!shas.containsKey(sha)) {
				System.err.println("patch ID " + sha + " was not found");
				continue;
			}
			int ver = shas.get(sha).version;
			Set<String> dirs = shas.get(sha).dirs;
			System.out.print("load sha ID " + Integer.toString(sha) + "(" + vers.get(ver) + "):");
			for (String dir: dirs) {
				System.out.print(" " + dir);
			}
			System.out.println();
			for (Element e: docReader.val().all()) { //p(topic|doc)
				Map<Integer, Double> verMap; //<ver time, current p(topic|ver)> for topicID (== e.index())
				Map<String, Double> dirMap; //<dir, current p(topic|dir)> for topicID ( == e.index())
				if (pTopicVer.containsKey(e.index())) {
					verMap = pTopicVer.get(e.index());
					dirMap = pTopicDir.get(e.index());
				} else {
					verMap = new TreeMap<Integer, Double>();
					dirMap = new HashMap<String, Double>();
					pTopicVer.put(e.index(), verMap);
					pTopicDir.put(e.index(), dirMap);
				}
				if (!verMap.containsKey(ver)) {
					verMap.put(ver, 0.0);
				}
				//p(topic|ver) = sigma{p(topic|doc) / num(doc|ver)}
				verMap.put(ver, verMap.get(ver) + e.get() / (double)verDocs.get(ver));
				for (String dir: dirs) {
					if (!dirMap.containsKey(dir)) {
						dirMap.put(dir, 0.0);
					}
					//p(topic|dir) = sigma{p(topic|doc) / num(doc|dir)}
					dirMap.put(dir, dirMap.get(dir) + e.get() / (double)dirDocs.get(dir));
				}
			}
		}
		docReader.close();

		File versionFile = new File(outputFile, "version");
		if (versionFile.exists()) {
			FileUtils.delete(versionFile, FileUtils.RECURSIVE);
		}
		versionFile.mkdirs();
		System.out.println("Write topic trends by kernel versions: " + versionFile.getAbsolutePath());
		for (Entry<Integer, Map<Integer, Double>> e: pTopicVer.entrySet()) {
			PrintWriter pw = JobUtils.getPrintWriter(new File(versionFile, e.getKey() + "-" + topicNames.get(e.getKey()) + ".csv"));
			for (Entry<Integer, Double> ver: e.getValue().entrySet()) {
				pw.println(vers.get(ver.getKey()) + "," + ver.getValue().toString());
			}
			pw.close();
		}

		File dirFile = new File(outputFile, "directory");
		if (dirFile.exists()) {
			FileUtils.delete(dirFile, FileUtils.RECURSIVE);
		}
		dirFile.mkdirs();
		System.out.println("Write topic trends by kernel directories: " + dirFile.getAbsolutePath());
		for (Entry<Integer, Map<String, Double>> e: pTopicDir.entrySet()) {
			PrintWriter pw = JobUtils.getPrintWriter(new File(dirFile, e.getKey() + "-" + topicNames.get(e.getKey()) + ".csv"));
			for (Entry<String, Double> ver: e.getValue().entrySet()) {
				pw.println(ver.getKey() + "," + ver.getValue().toString());
			}
			pw.close();
		}
	}

	private Map<Integer, GitMetaInfo> getGitMetaInfo(Path docIndexPath, FileSystem fs, 
			Repository repo, Map<Integer, String> vers, Map<Integer, Integer> verDocs, Map<String, Integer> dirDocs) throws Exception {
		System.out.println("Load docIndex");
		Map<String, Integer> revDocIndex = new HashMap<String, Integer>();
		SequenceDirectoryReader<Integer, String> docIndexReader = new SequenceDirectoryReader<>(docIndexPath, fs, Integer.class, String.class);
		while (docIndexReader.seekNext()) {
			if (docIndexReader.val().indexOf('-') != -1) {
				revDocIndex.put(docIndexReader.val().substring(0, docIndexReader.val().lastIndexOf('-')), docIndexReader.key());
			} else {
				revDocIndex.put(docIndexReader.val(), docIndexReader.key());
			}
		}
		docIndexReader.close();

		Map<Integer, GitMetaInfo> shas = new HashMap<Integer, GitMetaInfo>(); //<sha ID, version>
		Git git = new Git(repo);
		Iterator<RevCommit> logs = getIterator(repo, git, "", "HEAD", null);
		while (logs.hasNext()) {
			RevCommit rev = logs.next();
			if (rev.getParentCount() > 1) {//--no-merges
				continue;
			}
			int time = rev.getCommitTime();
			int version = 0;
			for (int t: vers.keySet()) {
				if (time >= t) {
					version = t;
				}
			}

			Set<String> dirs = new HashSet<String>();
			ObjectId next = repo.resolve(rev.name() + "^");
			if (next == null) {
				System.err.println("Object not found: " + rev.name() + "^");
				continue;
			}
			CanonicalTreeParser p = new CanonicalTreeParser();
			RevWalk walk = new RevWalk(repo);
			p.reset(repo.newObjectReader(), walk.parseTree(rev));
			CanonicalTreeParser p2 = new CanonicalTreeParser();
			p2.reset(repo.newObjectReader(), walk.parseTree(next));
			walk.close();
			for (DiffEntry diff: git.diff().setShowNameAndStatusOnly(true).setNewTree(p).setOldTree(p2).call()) {
				File file = null;
				switch(diff.getChangeType()) {
				case ADD: case COPY: case RENAME:
					file = new File(diff.getNewPath());
					break;
				case DELETE: case MODIFY:
					file = new File(diff.getOldPath());
				}
				file = file.getParentFile();
				while (file != null) {
					dirs.add(file.getPath());
					file = file.getParentFile();
				}
			}

			System.out.print("load sha " + rev.getName() + "(" + vers.get(version) + "):");
			shas.put(revDocIndex.get(rev.getName()), new GitMetaInfo(version, dirs));
			if (verDocs.containsKey(version)) {
				verDocs.put(version, verDocs.get(version) + 1);
			} else {
				verDocs.put(version, 1);
			}
			for (String dir: dirs) {
				System.out.print(" " + dir);
				if (dirDocs.containsKey(dir)) {
					dirDocs.put(dir, dirDocs.get(dir) + 1);
				} else {
					dirDocs.put(dir, 1);
				}
			}
			System.out.println();
		}
		return shas;
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
