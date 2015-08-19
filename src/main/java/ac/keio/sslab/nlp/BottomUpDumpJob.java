package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;

import ac.keio.sslab.clustering.bottomup.HierarchicalCluster;
import ac.keio.sslab.clustering.bottomup.HierarchicalClusterGraph;
import ac.keio.sslab.clustering.bottomup.HierarchicalClusterPrioritizer;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class BottomUpDumpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomupDump";
	}

	@Override
	public String getJobDescription() {
		return "generate .csv & .dot files for a bottomup job result";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID for a bottomup job"));
		OptionGroup g2 = new OptionGroup();
		g2.addOption(new Option("l", "ldaID", true, "ID for a lda job"));
		OptionGroup g3 = new OptionGroup();
		g3.addOption(new Option("d", "distance", true, "distance measure (Euclidean or Cosine)"));
		OptionGroup g4 = new OptionGroup();
		g4.addOption(new Option("c", "corpusID", true, "ID for a corpus job"));
		g.setRequired(true);
		g2.setRequired(true);
		g3.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g2);
		opt.addOptionGroup(g3);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = mgr.getLocalArgFile(conf.localBottomupFile, "j");
		File corpusDir = mgr.getLocalArgFile(conf.localCorpusFile, "c");
		File idIndexFile = new File(corpusDir, "idIndex.csv");
		File clustersFile = new File(localOutputDir, "clusters.csv");
		File priorityFile = new File(localOutputDir, "cluster_priority.txt");
		File mergingMergedFile = new File(localOutputDir.getAbsolutePath(), "mergingToFrom.seq");

		String distance = mgr.getArgStr("d");
		DistanceMeasure measure;
		if (distance.toLowerCase().equals("cosine")) {
			measure = new CosineDistanceMeasure();
		} else if (distance.toLowerCase().equals("euclidean")) {
			measure = new SquaredEuclideanDistanceMeasure();
		} else {
			System.err.println("Specify Cosine or Euclidean ");
			return;
		}

		if (mgr.doForceWrite()) {
			clustersFile.delete();
		}
		try {
			if (!clustersFile.exists()) {
				Map<Integer, String> topics = new HashMap<Integer, String>();
				for (Entry<Integer, List<String>> e: new TopicReader(ldaFiles.dictionaryPath, ldaFiles.topicPath, conf.hdfs, 2).getTopics().entrySet()) {
					topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
				}
				HierarchicalClusterGraph graph = HierarchicalClusterGraph.parseMergingMergedFile(mergingMergedFile, ldaFiles.documentPath, conf.hdfs);
				graph.setCentroidString(topics);
				graph.setDensity(measure);
				graph.dumpCSV(localOutputDir);
			} else {
				System.out.println("Found past result and reuse it. if you want to overwrite, then specify -ow");
			}
			HierarchicalClusterPrioritizer p = new HierarchicalClusterPrioritizer(clustersFile);
			PrintWriter writer = JobUtils.getPrintWriter(priorityFile);
			Map<Integer, List<String>> realId = getRealID(idIndexFile);
			int i = 1;
			for (Entry<Double, HierarchicalCluster> e: p.getOrder().entrySet()) {
				HierarchicalCluster c = e.getValue();
				writer.println("====================================================");
				writer.println("No. " + i + "\n");
				writer.println("Cluster ID = " + c.getID());
				writer.println("Parent cluster ID = " + (c.getParent() != null ? c.getParent().getID(): "root"));
				writer.println("Child cluster ID + " + (c.getLeft() != null ? c.getLeft().getID(): "leaf"));
				writer.println("Child cluster ID + " + (c.getRight() != null ? c.getRight().getID(): "leaf"));
				writer.println("Size: " + c.size());
				writer.println("Centroid Topic: " + c.getCentroidString());
				writer.println("Density: " + c.getDensity() + ", Score: " + e.getKey() + " (== Size / maxSize ( ==" + p.getMaxSize() + ") * (parent Density - Density / parent Density)");
				writer.println("Points:");
				for (int pointID: c.getPoints()) {
					writer.print(pointID + ": ");
					for (String r: realId.get(pointID)) {
						writer.print(r + " ");
					}
					if (realId.get(pointID).size() > 1) {
						writer.print("(duplicated)");
					}
					writer.println();
				}
				// TODO: add oneline and body, and JSON output
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<Integer, List<String>> getRealID(File input) throws IOException {
		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String [] s = line.split(",");
			List<String> a = new ArrayList<String>();
			for (int i = 1; i < s.length; i++) {
				a.add(s[i]);
			}
			ret.put(Integer.parseInt(s[0]), a);
		}
		reader.close();

		return ret;
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
