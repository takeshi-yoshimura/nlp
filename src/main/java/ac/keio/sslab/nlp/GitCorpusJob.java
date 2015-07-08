package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class GitCorpusJob implements NLPJob {

	public static final String delimiter = " ";
	NLPConf conf = NLPConf.getInstance();

	@Override
	public String getJobName() {
		return NLPConf.gitCorpusJobName;
	}

	@Override
	public String getJobDescription() {
		return "Filter and upload logs in the specified git repository with the specified ref.";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("g", "gitID", true, "ID of a git repository");
		options.addOption("s", "since", true, "Start object ref to be uploaded. Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded. Default is HEAD.");
		options.addOption("f", "file", true, "target file or directory path in git repository. Default is the top of the input directory.");
		options.addOption("sl", "stableLinux", false, "Get all the commits for stable Linux (if specified, ignore -s and -u)");
		options.addOption("c", "commitFile", true, "File for commits to be extracted");
		return options;
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
		return logs;
	}

	protected void iterateAndWrite(Iterator<RevCommit> logs, SequenceFile.Writer writer, Set<String> commits) throws Exception {
		MyAnalyzer analyzer = new MyAnalyzer();
		Text key = new Text(); Text value = new Text();
		while (logs.hasNext()) {
			RevCommit rev = logs.next();
			if (rev.getParentCount() > 1) {//--no-merges
				continue;
			}
			if (commits != null && !commits.contains(rev.getId().getName()))
				continue;
			System.err.println("Writing commit " + rev.getId().getName());
			key.set(rev.getId().getName());
			
			StringBuilder preprocessed = new StringBuilder();
			for (String para: rev.getFullMessage().split("\n\n")) {
				if (para.toLowerCase().indexOf("signed-off-by:") != -1 || para.toLowerCase().indexOf("cc:") != -1) {
					continue;
				}
				preprocessed.append(para);
				preprocessed.append(' ');
			}

			StringBuilder filtered = new StringBuilder();
			Reader reader = new StringReader(preprocessed.toString());
	        TokenStream stream = analyzer.tokenStream("", reader);
            CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
            stream.reset();

	        while (stream.incrementToken()) {
	        	String word = term.toString();
				if (word.matches("[0-9]+")) {
					continue;
				} else if ((word.matches("[a-f0-9]+") && !word.matches("[a-f]+")) || (word.matches("0x[a-f0-9]+"))) {
					continue;
				}
				filtered.append(word.toLowerCase());
				filtered.append(delimiter);
	        }
	        stream.end();
	        stream.close();
			if (filtered.length() >= delimiter.length()) {
				//strip the last delimiter
				filtered.setLength(filtered.length() - delimiter.length());
				//do not touch key
				value.set(filtered.toString());
				writer.append(key, value);
			}
		}
		analyzer.close();
	}
	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("g")) {
			System.err.println("Need to specify --gitID");
			return;
		}
		File inputDir = new File(conf.localGitFile, args.get("g"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		String sinceStr = "";
		if (args.containsKey("s")) {
			sinceStr = args.get("s");
		}
		String untilStr = "HEAD";
		if (args.containsKey("u")) {
			untilStr = args.get("u");
		}
		String fileStr = null;
		if (args.containsKey("f")) {
			fileStr = args.get("f");
		}
		Set<String> commits = null;
		if (args.containsKey("c")) {
			try {
				commits = new HashSet<String>();
				File file = new File(args.get("c"));
				BufferedReader br = new BufferedReader(new FileReader(file));
				String str = br.readLine();
				while(str != null) {
					commits.add(str);
					str = br.readLine();
				}
				br.close();
			} catch (Exception e) {
				System.err.println("Failed to load " + args.get("c"));
				return;
			}
		}

		Repository repo = null;
		Git git = null;
		SequenceFile.Writer writer = null;
		try {
			Configuration hdfsConf = new Configuration();
			FileSystem fs = FileSystem.get(hdfsConf);
			if (fs.exists(outputPath)) {
				if (args.containsKey("ow")) {
					if (!JobUtils.promptDeleteDirectory(fs, outputPath, args.containsKey("force"))) {
						throw new Exception("Overwrite revoked");
					}
				} else {
					throw new Exception(outputPath.toString() + " exists. You might want to use --overwrite.");
				}
			}
			Path tmpOutputPath = new Path(conf.tmpPath, outputPath.toString());
			SequenceFile.Writer.Option fileOpt = SequenceFile.Writer.file(tmpOutputPath);
			SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
			SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);
			fs.mkdirs(tmpOutputPath.getParent());
			writer = SequenceFile.createWriter(hdfsConf, fileOpt, keyClass, valueClass);

			repo = new FileRepositoryBuilder().findGitDir(inputDir).build();
			git = new Git(repo);
			if (args.containsKey("sl")) {
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
				Map<String, String> lts = new HashMap<String, String>();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
				for (Entry<String, String> e2: rangeMap.entrySet()) {
					ObjectId sinceRef = repo.resolve(e2.getKey());
					ObjectId untilRef = repo.resolve(e2.getValue());
					if (sinceRef == null || untilRef == null) {
						continue;
					}
					long since = walk.parseCommit(sinceRef).getCommitTime();
					long until = walk.parseCommit(untilRef).getCommitTime();
					if ((until - since) / 3600 / 24 > 365) {
						lts.put(e2.getKey() + " - " + e2.getValue(), 
								sdf.format(new Date(since * 1000)) + " - " + sdf.format(new Date(until * 1000)));
						Iterator<RevCommit> logs = getIterator(repo, git, e2.getKey(), e2.getValue(), null);
						iterateAndWrite(logs, writer, commits);
					}
				}
				System.out.println("Detected long-term stable versions:");
				PrintWriter pw = JobUtils.getPrintWriter(conf.finalStableCommitStatsFile);
				for (Entry<String, String> e3: lts.entrySet()) {
					System.out.println(e3.getKey() + ": " + e3.getValue());
					pw.println(e3.getKey() + ": " + e3.getValue());
				}
				pw.close();
			} else {
				Iterator<RevCommit> logs = getIterator(repo, git, sinceStr, untilStr, fileStr);
				iterateAndWrite(logs, writer, commits);
			}
			fs.mkdirs(outputPath.getParent());
			fs.rename(tmpOutputPath, outputPath);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Writing logs to HDFS failed");
			System.err.println("Repository path: " + inputDir);
			if (args.containsKey("sl")) {
				System.err.println("Ref: Stable Linux");
			} else {
				System.err.println("REF: --since " + sinceStr + " --until " + untilStr);
			}
			if (fileStr != null) {
				System.err.println("File: " + fileStr);
			}
		}
		if (git != null) {
			git.close();
		}
		if (repo != null) {
			repo.close();
		}
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("writer.close failed");
			}
		}
	}

	@Override
	public void takeSnapshot() {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			conf.localCorpusFile.mkdirs();
			for (FileStatus stat: fs.listStatus(conf.corpusPath)) {
				fs.copyToLocalFile(stat.getPath(), new Path(conf.localCorpusFile.getAbsolutePath(), stat.getPath().getName()));
			}
		} catch (Exception e) {
			System.err.println("Taking snapshot failed at " + getJobName() + ": " + e.toString());
		}
	}

	@Override
	public void restoreSnapshot() {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.mkdirs(conf.corpusPath);
			for (File localOutputFile: conf.localLdaFile.listFiles()) {
				fs.copyFromLocalFile(new Path(localOutputFile.getAbsolutePath()), new Path(conf.corpusPath, localOutputFile.getName()));
			}
		} catch (Exception e) {
			System.err.println("Restoring snapshot failed at " + getJobName() + ": " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
