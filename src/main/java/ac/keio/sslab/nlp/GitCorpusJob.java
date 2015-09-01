package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.corpus.DocumentFilter;
import ac.keio.sslab.nlp.corpus.GitCorpusReader;
import ac.keio.sslab.nlp.corpus.GitLogCorpusReader;
import ac.keio.sslab.nlp.corpus.ShaFileGitCorpusReader;
import ac.keio.sslab.nlp.corpus.StableLinuxGitCorpusReader;
import ac.keio.sslab.utils.hadoop.SequenceSwapWriter;

public class GitCorpusJob implements NLPJob {

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
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("g", "gitDir", true, "path to a git repository"));
		g.setRequired(true);

		Options options = new Options();
		options.addOptionGroup(g);
		options.addOption("s", "since", true, "Start object ref to be uploaded (yyyy/MM/dd). Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded (yyyy/MM/dd). Default is HEAD.");
		options.addOption("f", "file", true, "target file or directory path in git repository. Default is the top of the input directory.");
		options.addOption("sl", "stableLinux", false, "Get all the commits for stable Linux (if specified, ignore -s and -u)");
		options.addOption("c", "commitFile", true, "File for commits to be extracted");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is false)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		options.addOption("p", "splitParagraph", true, "split paragraphs? (default is false)");
		return options;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		File inputDir = new File(mgr.getArgStr("g"));
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		String sinceStr = mgr.getArgOrDefault("s", "", String.class);
		String untilStr = mgr.getArgOrDefault("u", "HEAD", String.class);
		String fileStr = mgr.getArgOrDefault("f", null, String.class);
		boolean tokenizeAtUnderline = mgr.getArgOrDefault("t", false, Boolean.class);
		boolean useNLTKStopwords = mgr.getArgOrDefault("n", false, Boolean.class);
		boolean splitParagraph = mgr.getArgOrDefault("p", false, Boolean.class);

		try {
			GitCorpusReader reader = null;
			if (mgr.hasArg("c")) {
				reader = new ShaFileGitCorpusReader(new File(mgr.getArgStr("c")), inputDir);
			} else if (mgr.hasArg("sl")) {
				reader = new StableLinuxGitCorpusReader(inputDir, fileStr);
			} else {
				reader = new GitLogCorpusReader(inputDir, sinceStr, untilStr, fileStr, false);
			}

			File stats = new File(conf.localCorpusFile, mgr.getJobID() + "/stats.txt");
			File commits = new File(stats.getParent(), "commits.txt");
			File idIndexFile = new File(stats.getParent(), "idIndex.txt");
			File originalCorpus = new File(stats.getParent(), "beforesStopWordsCorpus.txt");
			File dfFile = new File(stats.getParent(), "df.txt");
			if (stats.exists()) {
				stats.delete();
				commits.delete();
				idIndexFile.delete();
			}
			PrintWriter commitsWriter = JobUtils.getPrintWriter(commits);
			PrintWriter originalCorpusWriter = JobUtils.getPrintWriter(originalCorpus);

			DocumentFilter filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);
			// <content sha, id for content sha>
			Map<String, Integer> contentShas = new HashMap<String, Integer>();
			// <id for content sha, [commit shas]>
			Map<Integer, List<String>> idIndex = new TreeMap<Integer, List<String>>();
			Map<String, Integer> df = new HashMap<String, Integer>();
			int i = 0;
			int totalCommits = 0, totalDocuments = 0;
			if (!splitParagraph) {
				StringBuilder sb = new StringBuilder("");
				while (reader.seekNext()) {
					sb.setLength(0);
					System.out.println("commit " + reader.getSha());
					totalCommits++;

					for (String para: filter.filterDocument(reader.getDoc())) {
						sb.append(para).append(' ');
					}
					if (sb.length() == 0) {
						continue;
					}
					sb.setLength(sb.length() - 1);
					if (!sb.toString().contains(" ")) {
						continue;
					}

					commitsWriter.print(reader.getSha());
					commitsWriter.print(',');
					commitsWriter.print(reader.getDate());
					commitsWriter.print(',');
					commitsWriter.print(reader.getVersion());
					for (String version: reader.getFiles()) {
						commitsWriter.print(',');
						commitsWriter.print(version);
					}
					commitsWriter.println();

					String contentSha = JobUtils.getSha(sb.toString());
					if (!contentShas.containsKey(contentSha)) {
						idIndex.put(i, new ArrayList<String>());
						contentShas.put(contentSha, i);
						originalCorpusWriter.print(Integer.toString(i++));
						originalCorpusWriter.print(' ');
						originalCorpusWriter.println(sb.toString());

						Set<String> words = new HashSet<String>();
						for (String word: sb.toString().split(" ")) {
							words.add(word);
						}
						for (String word: words) {
							if (!df.containsKey(word)) {
								df.put(word, 0);
							}
							df.put(word, df.get(word) + 1);
						}
					}
					idIndex.get(contentShas.get(contentSha)).add(reader.getSha());
					totalDocuments++;
				}
			} else {
				while (reader.seekNext()) {
					int pId = 0;
					System.out.println("commit " + reader.getSha() + "," + reader.getDate() + "," + reader.getVersion());
					boolean hasParagraph = false;
					for (String para: filter.filterDocument(reader.getDoc())) {
						if (!para.contains(" ")) {
							continue;
						}

						if (!para.isEmpty()) {
							hasParagraph = true;
						}
						String contentSha = JobUtils.getSha(para);
						if (!contentShas.containsKey(contentSha)) {
							idIndex.put(i, new ArrayList<String>());
							contentShas.put(contentSha, i);
							originalCorpusWriter.print(Integer.toString(i++));
							originalCorpusWriter.print(' ');
							originalCorpusWriter.println(para);

							Set<String> words = new HashSet<String>();
							for (String word: para.split(" ")) {
								words.add(word);
							}
							for (String word: words) {
								if (!df.containsKey(word)) {
									df.put(word, 0);
								}
								df.put(word, df.get(word) + 1);
							}
						}
						idIndex.get(contentShas.get(contentSha)).add(reader.getSha() + "-" + pId++);
						totalDocuments++;
					}
					if (!hasParagraph) {
						continue;
					}
					totalCommits++;
					commitsWriter.print(reader.getSha());
					commitsWriter.print(',');
					commitsWriter.print(reader.getDate());
					commitsWriter.print(',');
					commitsWriter.print(reader.getVersion());
					for (String version: reader.getFiles()) {
						commitsWriter.print(',');
						commitsWriter.print(version);
					}
					commitsWriter.println();
				}
			}
			commitsWriter.close();
			originalCorpusWriter.close();

			Comparator<Entry<String, Integer>> reverser = new Comparator<Entry<String, Integer>>() {
				public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
					return e2.getValue().compareTo(e1.getValue());
				}
			};
			List<Entry<String, Integer>> orderedDf = new ArrayList<Entry<String, Integer>>(df.entrySet());
			Set<String> stopWord = new HashSet<String>();
			Collections.sort(orderedDf, reverser);
			PrintWriter dfWriter = JobUtils.getPrintWriter(dfFile);
			for (Entry<String, Integer> e: orderedDf) {
				if (e.getValue() > idIndex.size() / 5) {
					stopWord.add(e.getKey());
					dfWriter.print("[stop word] ");
				}
				dfWriter.print(e.getKey());
				dfWriter.print(',');
				dfWriter.println(e.getValue());
			}
			dfWriter.close();

			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, conf.hdfs, mgr.doForceWrite(), String.class, String.class);
			BufferedReader br = new BufferedReader(new FileReader(originalCorpus));
			String line = null;
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
				String [] splitLine = line.split(" ");
				sb.setLength(0);
				for (int j = 1; j < splitLine.length; j++) {
					if (stopWord.contains(splitLine[j])) {
						continue;
					}
					sb.append(splitLine[j]).append(' ');
				}
				sb.setLength(sb.length() - 1);
				writer.append(splitLine[0], sb.toString());
			}
			br.close();
			writer.close();

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
			reader.close();

			PrintWriter idIndexWriter = JobUtils.getPrintWriter(idIndexFile);
			for (Entry<Integer, List<String>> id: idIndex.entrySet()) {
				sb.setLength(0);
				sb.append(id.getKey()).append('\t').append('\t');
				for (String sha: id.getValue()) {
					sb.append(sha).append(',');
				}
				sb.setLength(sb.length() - 1);
				idIndexWriter.println(sb.toString());
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
