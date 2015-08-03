package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.Vector;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class TopDownDumpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "topdownDump";
	}

	@Override
	public String getJobDescription() {
		return "generate CSV for a dendrogram with a topdown result";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("t", "topdownID", true, "ID of topdown"));
		g.addOption(new Option("l", "ldaID", true, "ID of lda"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		Map<Integer, String> clusterStr = new HashMap<Integer, String>();
		Map<Integer, Integer> clusterCount = new HashMap<Integer, Integer>();
		try {
			LDAHDFSFiles hdfs = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
			Configuration hdfsConf = new Configuration();
			Map<Integer, String> topicStr = new HashMap<Integer, String>();
			TopicReader topReader = new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, hdfsConf, 1);
			StringBuilder sb = new StringBuilder();
			for (Entry<Integer, List<String>> topic: topReader.getTopics().entrySet()) {
				sb.setLength(0);
				sb.append("T").append(topic.getKey());
				for (String word: topic.getValue()) {
					sb.append('-').append(word);
				}
				topicStr.put(topic.getKey(), sb.toString());
			}

			FileSystem fs = FileSystem.get(hdfsConf);
			Path topdownPath = mgr.getArgJobIDPath(conf.topdownPath, "t");
			Comparator<Entry<Integer, Double>> reverser = new Comparator<Entry<Integer, Double>>() {
				public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
					return e2.getValue().compareTo(e1.getValue());
				}
			};

			for (FileStatus status: fs.listStatus(topdownPath)) {
				Path dirPath = status.getPath();
				if (!dirPath.getName().startsWith("topdown-")) {
					continue;
				}

				Path clusteredPoints = new Path(dirPath, "clusteredPoints");
				if (!fs.exists(clusteredPoints)) {
					continue;
				}
				FileStatus [] iterationSeqs = fs.globStatus(new Path(dirPath, "iteration-*-final.seq"));
				if (iterationSeqs.length != 1) {
					continue;
				}
				Path centroidsPath = iterationSeqs[0].getPath();
				SequenceDirectoryReader<Integer, Cluster> centroidReader = new SequenceDirectoryReader<>(centroidsPath, hdfsConf, Integer.class, Cluster.class);

				while (centroidReader.seekNext()) {
					Map<Integer, Double> tmpMap = new HashMap<Integer, Double>();
					for (Element e: centroidReader.val().getCenter().all()) {
						tmpMap.put(e.index(), e.get());
					}
					List<Entry<Integer, Double>> sorted = new ArrayList<Entry<Integer, Double>>(tmpMap.entrySet());
					Collections.sort(sorted, reverser);
					sb.setLength(0);
					sb.append(topicStr.get(sorted.get(0).getKey())).append(':').append(sorted.get(0).getValue());
					sb.append(',').append(topicStr.get(sorted.get(1).getKey())).append(':').append(sorted.get(1).getValue());
					clusterStr.put(centroidReader.key(), sb.toString());
				}

				SequenceDirectoryReader<Integer, Vector> seq = new SequenceDirectoryReader<>(clusteredPoints, hdfsConf, Integer.class, Vector.class);
				while (seq.seekNext()) {
					if (!clusterCount.containsKey(seq.key())) {
						clusterCount.put(seq.key(), 0);
					}
					clusterCount.put(seq.key(), clusterCount.get(seq.key()) + 1);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		File outDir = new File(conf.finalOutputFile, "topdown/" + mgr.getArgStr("t"));
		if (outDir.exists()) {
			try {
				FileUtils.delete(outDir, FileUtils.RECURSIVE);
			} catch (Exception e) {
				System.err.println("failed to delete " + outDir.getAbsolutePath());
				return;
			}
		}
		File tmpOutputFile = new File(NLPConf.tmpDirName, "topdown/" + mgr.getArgStr("t"));
		tmpOutputFile.mkdirs();
		tmpOutputFile.deleteOnExit();

		File centroidFile = new File(tmpOutputFile, "centroids.csv");
		try {
			PrintWriter pw = JobUtils.getPrintWriter(centroidFile);
			StringBuilder sb = new StringBuilder();
			for (Entry<Integer, Integer> e: clusterCount.entrySet()) {
				sb.setLength(0);
				int clusterID = e.getKey();
				sb.append(clusterID).append(',').append(String.format("%1$3f", e.getValue()));
				sb.append(',').append(clusterStr.get(clusterID));
				pw.println(sb.toString());
			}
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		outDir.mkdirs();
		tmpOutputFile.renameTo(outDir);
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
