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

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class TopDownDumpJob implements NLPJob {
	NLPConf conf = NLPConf.getInstance();

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
		Options opt = new Options();
		opt.addOption("t", "topdownID", true, "ID of topdown");
		opt.addOption("l", "ldaID", true, "ID of lda");
		return opt;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("t") || !args.containsKey("l")) {
			System.err.println("Need to specify --topdownID and --ldaID");
			return;
		}

		Map<Integer, String> clusterStr = new HashMap<Integer, String>();
		Map<Integer, Integer> clusterCount = new HashMap<Integer, Integer>();
		try {
			LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
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
			Path topdownPath = new Path(conf.topdownPath, args.get("t"));
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
				SequenceDirectoryReader centroidReader = new SequenceDirectoryReader(centroidsPath, hdfsConf);
				IntWritable key = new IntWritable();
				ClusterWritable value = new ClusterWritable();

				while (centroidReader.next(key, value)) {
					Map<Integer, Double> tmpMap = new HashMap<Integer, Double>();
					for (Element e: value.getValue().getCenter().all()) {
						tmpMap.put(e.index(), e.get());
					}
					List<Entry<Integer, Double>> sorted = new ArrayList<Entry<Integer, Double>>(tmpMap.entrySet());
					Collections.sort(sorted, reverser);
					sb.setLength(0);
					sb.append(topicStr.get(sorted.get(0).getKey())).append(':').append(sorted.get(0).getValue());
					sb.append(',').append(topicStr.get(sorted.get(1).getKey())).append(':').append(sorted.get(1).getValue());
					clusterStr.put(key.get(), sb.toString());
				}

				SequenceDirectoryReader seq = new SequenceDirectoryReader(clusteredPoints, hdfsConf);
				VectorWritable value2 = new VectorWritable();
				while (seq.next(key, value2)) {
					if (!clusterCount.containsKey(key.get())) {
						clusterCount.put(key.get(), 0);
					}
					clusterCount.put(key.get(), clusterCount.get(key.get()) + 1);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		File outDir = new File(conf.finalOutputFile, "topdown/" + args.get("t"));
		if (outDir.exists()) {
			try {
				FileUtils.delete(outDir, FileUtils.RECURSIVE);
			} catch (Exception e) {
				System.err.println("failed to delete " + outDir.getAbsolutePath());
				return;
			}
		}
		File tmpOutputFile = new File(NLPConf.tmpDirName, "topdown/" + args.get("t"));
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
