package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.corpus.DocumentFilter;
import ac.keio.sslab.nlp.corpus.GitCorpusReader;
import ac.keio.sslab.nlp.corpus.GitLogCorpusReader;
import ac.keio.sslab.nlp.corpus.ShaFileGitCorpusReader;
import ac.keio.sslab.nlp.corpus.StableLinuxGitCorpusReader;

public class GitCorpusJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

	@Override
	public String getJobName() {
		return "gitCorpus";
	}

	@Override
	public String getJobDescription() {
		return "Filter and upload logs in the specified git repository with the specified ref.";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("g", "gitDir", true, "path to a git repository");
		options.addOption("s", "since", true, "Start object ref to be uploaded (yyyy/MM/dd). Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded (yyyy/MM/dd). Default is HEAD.");
		options.addOption("f", "file", true, "target file or directory path in git repository. Default is the top of the input directory.");
		options.addOption("sl", "stableLinux", false, "Get all the commits for stable Linux (if specified, ignore -s and -u)");
		options.addOption("c", "commitFile", true, "File for commits to be extracted");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is true)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		options.addOption("p", "splitParagraph", true, "split paragraphs? (default is false)");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("g")) {
			System.err.println("Need to specify --gitDir");
			return;
		}
		File inputDir = new File(args.get("g"));
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
		boolean tokenizeAtUnderline = false;
		if (args.containsKey("t")) {
			tokenizeAtUnderline = Boolean.parseBoolean(args.get("t"));
		}
		boolean useNLTKStopwords = false;
		if (args.containsKey("n")) {
			useNLTKStopwords = Boolean.parseBoolean(args.get("n"));
		}

		try {
			GitCorpusReader reader = null;
			if (args.containsKey("c")) {
				reader = new ShaFileGitCorpusReader(new File(args.get("c")), inputDir);
			} else if (args.containsKey("sl")) {
				reader = new StableLinuxGitCorpusReader(inputDir, fileStr);
			} else {
				reader = new GitLogCorpusReader(inputDir, sinceStr, untilStr, fileStr);
			}

			File stats = new File(conf.localCorpusFile, args.get("j") + "/stats.txt");
			File commits = new File(stats.getParent(), "commits.txt");
			File idIndexFile = new File(stats.getParent(), "idIndex.txt");
			if (stats.exists()) {
				stats.delete();
				commits.delete();
				idIndexFile.delete();
			}
			PrintWriter commitsWriter = JobUtils.getPrintWriter(commits);

			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, new Configuration(), args.containsKey("ow"), String.class, String.class);
			DocumentFilter filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);
			// <content sha, id for content sha>
			Map<String, Integer> contentShas = new HashMap<String, Integer>();
			// <id for content sha, [commit shas]>
			Map<Integer, List<String>> idIndex = new TreeMap<Integer, List<String>>();
			int i = 0;
			int totalCommits = 0, totalDocuments = 0;
			boolean splitParagraph = args.containsKey("p") && Boolean.parseBoolean(args.get("p"));
			if (!splitParagraph) {
				StringBuilder sb = new StringBuilder("");
				while (reader.seekNext()) {
					sb.setLength(0);
					System.out.println("commit " + reader.getSha());
					totalCommits++;
					commitsWriter.println(reader.getSha());
					for (String para: filter.filterDocument(reader.getDoc())) {
						sb.append(para).append(' ');
					}
					String contentSha = JobUtils.getSha(sb.toString());
					if (!contentShas.containsKey(contentSha)) {
						idIndex.put(i, new ArrayList<String>());
						contentShas.put(contentSha, i);
						writer.append(Integer.toString(i++), sb.toString());
					}
					idIndex.get(contentShas.get(contentSha)).add(reader.getSha());
					totalDocuments++;
				}
			} else {
				while (reader.seekNext()) {
					int pId = 0;
					System.out.println("commit " + reader.getSha());
					totalCommits++;
					commitsWriter.println(reader.getSha());
					for (String para: filter.filterDocument(reader.getDoc())) {
						String contentSha = JobUtils.getSha(para);
						if (!contentShas.containsKey(contentSha)) {
							idIndex.put(i, new ArrayList<String>());
							contentShas.put(contentSha, i);
							writer.append(Integer.toString(i++), para);
						}
						idIndex.get(contentShas.get(contentSha)).add(reader.getSha() + "-" + pId++);
						totalDocuments++;
					}
				}
			}
			PrintWriter statsWriter = JobUtils.getPrintWriter(stats);
			statsWriter.println(reader.getStats());
			statsWriter.print("total commits: ");
			statsWriter.println(totalCommits);
			statsWriter.print("total documents:");
			statsWriter.println(totalDocuments);
			statsWriter.print("total documents after deduplication: ");
			statsWriter.println(idIndex.size());
			statsWriter.print("tokenize at underline?: ");
			statsWriter.println(tokenizeAtUnderline);
			statsWriter.print("use NLTK stopword?: ");
			statsWriter.println(useNLTKStopwords);
			statsWriter.print("split paragraph?: ");
			statsWriter.println(splitParagraph);
			statsWriter.close();
			writer.close();
			reader.close();
			commitsWriter.close();

			PrintWriter idIndexWriter = JobUtils.getPrintWriter(idIndexFile);
			for (Entry<Integer, List<String>> id: idIndex.entrySet()) {
				idIndexWriter.print(id.getKey());
				idIndexWriter.print("\t\t");
				for (String sha: id.getValue()) {
					idIndexWriter.print(sha);
					idIndexWriter.print(',');
				}
				idIndexWriter.println();
			}
			idIndexWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
