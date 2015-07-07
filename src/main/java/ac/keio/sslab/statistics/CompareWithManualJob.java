package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector.Element;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
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
		
		System.out.println("Load manual results from " + manualFile.getAbsolutePath());
		Set<String> tagNames = new HashSet<String>();
		Map<String, Set<String>> manClass = new HashMap<String, Set<String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(manualFile));
			String line;
			while ((line = br.readLine()) != null) {
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
			FileUtils.delete(outDir, FileUtils.RECURSIVE);
		}
		File outputFile = new File(outDir, args.get("l"));
		
		
		
		
		
		
		
		
		
		
		
		
		System.out.println("Write topic trends by kernel versions: " + versionFile.getAbsolutePath());

		for (Entry<Integer, Map<Integer, Double>> e: pTopicVer.entrySet()) {
			PrintWriter pw = JobUtils.getPrintWriter(new File(versionFile, e.getKey() + "-" + topicNames.get(e.getKey()) + ".csv"));
			for (Entry<Integer, Double> ver: e.getValue().entrySet()) {
				pw.println(vers.get(ver.getKey()) + "," + ver.getValue().toString());
			}
			pw.close();
		}
	}

	@Override
	public void takeSnapshot() {
		// TODO Auto-generated method stub

	}

	@Override
	public void restoreSnapshot() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean runInBackground() {
		// TODO Auto-generated method stub
		return false;
	}

}
