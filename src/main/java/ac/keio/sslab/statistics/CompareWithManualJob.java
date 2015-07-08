package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.nlp.NLPConf;
import ac.keio.sslab.nlp.NLPJob;
import ac.keio.sslab.nlp.lda.DocumentReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class CompareWithManualJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();
	
	@Override
	public String getJobName() {
		return "compareWithManual";
	}

	@Override
	public String getJobDescription() {
		return "estimates F-measure with LDA and manual classifications";
	}

	@Override
	public Options getOptions() {
		Options opt = new Options();
		opt.addOption("m", "manualResult", true, "csv File {commits, tag1,tag2,....}");
		opt.addOption("t", "numTopicsForClass", true, "number of topics to use classification (default = 10)");
		opt.addOption("l", "ldaID", true, "ID for lda job");
		return opt;
	}

	class StringPair {
		public String tag, topic;
	}
	class IntTriple {
		public int TP = 0, FP = 0, FN = 0;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("l") || !args.containsKey("m")) {
			System.err.println("Need to specify --ldaID and --manualResult");
			return;
		}
		int numClassTopics = 10;
		if (args.containsKey("t")) {
			numClassTopics = Integer.parseInt(args.get("t"));
		}

		File manualFile = new File(args.get("m"));
		LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
		Configuration hdfsConf = new Configuration();

		System.out.println("Load topic");
		Map<Integer, String> topics = new HashMap<Integer, String>();
		try {
			for (Entry<Integer, List<String>> e: new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, hdfsConf, 2).getTopics().entrySet()) {
				topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
		} catch (Exception e) {
			System.err.println("Failed to load HDFS files " + hdfs.dictionaryPath + " or " + hdfs.topicPath);
		}
		System.out.println("Loaded " + topics.size() + " topics");
		
		System.out.println("Load manual results from " + manualFile.getAbsolutePath());
		Set<String> tagNames = new HashSet<String>();
		Map<String, Set<String>> manClass = new HashMap<String, Set<String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(manualFile));
			String line;
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				String[] splitLine = line.split(",");
				Set<String> tags = new HashSet<String>(Arrays.asList(splitLine));
				tags.remove(splitLine[0]);
				manClass.put(splitLine[0], tags);
				tagNames.addAll(tags);
			}
			br.close();
		} catch (Exception e) {
			System.err.println("Failed to load Local file " + manualFile.getAbsolutePath());
			return;
		}
		System.out.println("Loaded " + manClass.size() + " shas and " + tagNames.size() + " tags");

		System.out.println("Extracting documents with " + numClassTopics + " topics");
		Set<String> topicNames = new HashSet<String>();
		Map<String, Set<String>> ldaClass = new HashMap<String, Set<String>>();
		try {
			DocumentReader docReader = new DocumentReader(hdfs.docIndexPath, hdfs.documentPath, hdfsConf, numClassTopics);
			for (Entry<String, List<Integer>> document: docReader.getDocuments().entrySet()) {
				if (!manClass.containsKey(document.getKey()))
					continue;

				Set<String> topicStrs = new HashSet<String>();
				for (int topicId: document.getValue()) {
					topicStrs.add(topics.get(topicId));
				}
				ldaClass.put(document.getKey(), topicStrs);
				topicNames.addAll(topicStrs);
			}
		} catch (Exception e) {
			System.err.println("Failed to loead HDFS file " + hdfs.docIndexPath + " or " + hdfs.documentPath);
			return;
		}
		System.out.println("Loaded " + ldaClass.size() + " shas and " + topicNames.size() + " topics");
		
		
		Map<String, Map<String, IntTriple>> crossTables = new HashMap<String, Map<String, IntTriple>>();
		for (String sha: manClass.keySet()) {
			for (String tag: tagNames) {
				Map<String, IntTriple> forTagMap;
				if (!crossTables.containsKey(tag))
					crossTables.put(tag, new HashMap<String, IntTriple>());
				forTagMap = crossTables.get(tag);

				for (String topic: topicNames) {
					IntTriple tr;
					if (!forTagMap.containsKey(topic))
						forTagMap.put(topic, new IntTriple());
					tr = forTagMap.get(topic);
					
					if (!ldaClass.containsKey(sha)) {
						System.out.println("Dropped sha: " + sha);
						System.out.println("This can happen the message for sha is empty or a single paragraph with Signed-off-by or when all the words are stop-words");
						continue;
					}
					
					if (manClass.get(sha).contains(tag) && ldaClass.get(sha).contains(topic))
						tr.TP += 1;
					else if (manClass.get(sha).contains(tag) && !ldaClass.get(sha).contains(topic))
						tr.FP += 1;
					else if (!manClass.get(sha).contains(tag) && ldaClass.get(sha).contains(topic))
						tr.FN += 1;
				}
			}
		}

		File outDir = new File(conf.finalOutputFile, "compareWithManul");
		if (outDir.exists()) {
			try {
				FileUtils.delete(outDir, FileUtils.RECURSIVE);
			} catch (Exception e) {
				System.err.println("failed to delete " + outDir.getAbsolutePath());
				return;
			}
		}
		File outputFile = new File(outDir, args.get("l"));

		Comparator<Entry<String, Double>> reverser = new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		};
		try {
			PrintWriter pw = JobUtils.getPrintWriter(new File(outputFile, "FMeasure.csv"));
			pw.println("#tag,topic,F-measure,Precision(TP/(TP+FN)),Recall(TP/(TP+FP)),TP,FP,FN");
			StringBuilder sb = new StringBuilder();
			for (Entry<String, Map<String, IntTriple>> crossTable: crossTables.entrySet()) {
				String tag = crossTable.getKey();
				Map<String, Double> fmeasures = new HashMap<String, Double>();
				Map<String, Double> precisions = new HashMap<String, Double>();
				Map<String, Double> recalls = new HashMap<String, Double>();
	
				for (Entry<String, IntTriple> forTagEntry: crossTable.getValue().entrySet()) {
					String topic = forTagEntry.getKey();
					IntTriple tr = forTagEntry.getValue();
					if (tr.TP + tr.FP == 0 || tr.TP + tr.FN == 0 || tr.TP == 0) {
						continue;
					}
					double precision = (double)tr.TP / (tr.TP + tr.FN);
					double recall = (double)tr.TP / (tr.TP + tr.FP);
					precisions.put(topic, precision);
					recalls.put(topic, recall);
					fmeasures.put(topic, 2 * recall * precision / (recall + precision));
				}
				List<Entry<String, Double>> sorted = new ArrayList<Entry<String, Double>>(fmeasures.entrySet());
				Collections.sort(sorted, reverser);
				for (Entry<String, Double> e: sorted) {
					String topic = e.getKey();
					sb.append(tag).append(',').append(topic).append(e.getValue())
					  .append(',').append(precisions.get(topic))
					  .append(',').append(recalls.get(topic))
					  .append(',').append(crossTables.get(tag).get(topic).TP);
					pw.println(sb.toString());
					sb.setLength(0);
				}
			}
			pw.close();
		} catch (Exception e) {
			System.err.println("Failed to write " + new File(outputFile, "FMeasure.csv").getAbsolutePath());
		}

	}

	@Override
	public void takeSnapshot() {
	}

	@Override
	public void restoreSnapshot() {
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
