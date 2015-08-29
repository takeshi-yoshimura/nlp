package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.clustering.bottomup.CachedBottomupClustering;
import ac.keio.sslab.clustering.bottomup.HierarchicalCluster;
import ac.keio.sslab.clustering.bottomup.IndexBottomupClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;
import ac.keio.sslab.utils.mahout.SimpleLDAReader;

public class BottomUpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "group average hierarchical clustering";
	}

	@Override
	public Options getOptions() {
		OptionGroup required = new OptionGroup();
		required.addOption(new Option("l", "ldaID", true, "ID of lda"));
		required.setRequired(true);

		Options opt = new Options();
		opt.addOption("d", "dimension", false, "clustering for each dimension");
		opt.addOptionGroup(required);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = new File(conf.localBottomupFile, mgr.getJobID());
		File clustersFile = new File(localOutputDir, "clusters.csv");
		File dimensionFile = new File(localOutputDir, "dimensionCluster");
		File corpusIDIndexFile = new File(localOutputDir, "corpusIDIndex.csv");
		localOutputDir.mkdirs();

		try {
			Map<Integer, String> topicStr = SimpleLDAReader.getTopicTerm(conf.hdfs, ldaFiles.dictionaryPath, ldaFiles.topicPath);
			List<Vector> points = new ArrayList<Vector>();
			Map<Integer, String> docIndex = SimpleLDAReader.getDocIndex(conf.hdfs, ldaFiles.docIndexPath);
			Map<Integer, HierarchicalCluster> clusters = new HashMap<Integer, HierarchicalCluster>();

			SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(ldaFiles.documentPath, conf.hdfs, Integer.class, Vector.class);
			int nextClusterID = 0;
			PrintWriter corpusIndexW = JobUtils.getPrintWriter(corpusIDIndexFile);
			while (reader.seekNext()) {
				corpusIndexW.println(nextClusterID + "," + docIndex.get(reader.key()));
				HierarchicalCluster c = new HierarchicalCluster(nextClusterID, nextClusterID);
				clusters.put(nextClusterID, c);
				points.add(reader.val());
				c.setCentroid(points, topicStr);
				c.setDensity(0.0);
				nextClusterID++;
			}
			reader.close();
			corpusIndexW.close();

			if (!mgr.hasArg("d")) {
				CachedBottomupClustering clustering = new CachedBottomupClustering(points, 5L * 1024 * 1024 * 1024);
	
				PrintWriter writer = JobUtils.getPrintWriter(clustersFile);
				writer.println("#HierarchicalClusterID,size,density,parentID,leftCID,rightCID,centroid...,pointIDs...");
				int i = 0;
				int [] nextPair = null;
				HierarchicalCluster newC = null;
				while((nextPair = clustering.popMostSimilarClusterPair()) != null) {
					double similarity = clustering.getMaxSimilarity();
					System.out.println("Iteration #" + i++ + ": " + nextPair[0] + "," + nextPair[1]);
	
					HierarchicalCluster leftC = clusters.get(nextPair[0]);
					HierarchicalCluster rightC = clusters.get(nextPair[1]);
					newC = new HierarchicalCluster(leftC, rightC, nextClusterID++);
					newC.setDensity(similarity);
					newC.setCentroid(points, topicStr);
					clusters.put(nextPair[0], newC);
					clusters.remove(nextPair[1]);
	
					writer.println(leftC.toString());
					writer.println(rightC.toString());
					writer.flush();
				}
				writer.println(newC.toString());
				writer.close();
				System.out.println("Finished!");
			} else {
				int numCore = Runtime.getRuntime().availableProcessors();
				Thread t [] = new Thread[numCore];
				int tIndex [] = new int[numCore];
				int i = 0;
				for (int n = 0; n < numCore; n++) {
					System.out.println("Start: " + i);
					t[n] = new IndexBottomupClustering(points, i, new File(dimensionFile, i + ".csv"), topicStr.get(i));
					tIndex[n] = i;
					i++;
					t[n].start();
				}
				while (i < points.get(0).size()) {
					Thread.sleep(1000);
					for (int n = 0; n < numCore; n++) {
						if (!t[n].isAlive()) {
							System.out.println("Finished: " + tIndex[n]);
							System.out.println("Start: " + i);
							t[n] = new IndexBottomupClustering(points, i, new File(dimensionFile, i + ".csv"), topicStr.get(i));
							tIndex[n] = i;
							i++;
							t[n].start();
						}
					}
				}
				for (int n = 0; n < numCore; n++) {
					if (t[n].isAlive()) {
						t[n].join();
						System.out.println("Finished: " + tIndex[n]);
					}
				}
				System.out.println("Finished!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

}
