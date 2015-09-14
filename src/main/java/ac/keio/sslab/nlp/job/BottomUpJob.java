package ac.keio.sslab.nlp.job;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.analytics.HierarchicalCluster;
import ac.keio.sslab.clustering.bottomup.CachedBottomupClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;
import ac.keio.sslab.utils.mahout.SimpleLDAReader;

public class BottomUpJob extends ClusteringJobGroup implements NLPJob {

	@Override
	public NLPJobGroup getJobGroup() {
		return this;
	}

	@Override
	public Options getOptions() {
		return null;
	}

	@Override
	public String getAlgorithmName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "group average hierarchical clustering";
	}

	@Override
	public String getAlgorithmDescription() {
		return getJobDescription();
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(pMgr.getHDFSOutputDir());
		File localOutputDir = mgr.getLocalOutputDir();
		File corpusIDIndexFile = new File(localOutputDir, "corpusIDIndex.csv");
		localOutputDir.mkdirs();

		System.out.println("Start at: " + (new Date().toString()));
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

		runClustering(points, clusters, topicStr, localOutputDir);
		System.out.println("End at: " + (new Date().toString()));
	}

	public void runClustering(List<Vector> points, Map<Integer, HierarchicalCluster> clusters, Map<Integer, String> topicStr, File localOutputDir) throws Exception {
		File clustersFile = new File(localOutputDir, "clusters.csv");
		int nextClusterID = points.size();
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
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
