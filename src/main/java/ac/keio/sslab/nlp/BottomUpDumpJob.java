package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import ac.keio.sslab.clustering.bottomup.HierarchicalCluster;
import ac.keio.sslab.clustering.bottomup.HierarchicalClusterGraph;
import ac.keio.sslab.clustering.bottomup.HierarchicalClusterPrioritizer;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;
import ac.keio.sslab.utils.OrderedJson;

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
		OptionGroup g5 = new OptionGroup();
		g4.addOption(new Option("g", "gitDir", true, "git directory"));
		g.setRequired(true);
		g2.setRequired(true);
		g3.setRequired(true);
		g4.setRequired(true);
		g5.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g2);
		opt.addOptionGroup(g3);
		opt.addOptionGroup(g4);
		opt.addOptionGroup(g5);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = mgr.getLocalArgFile(conf.localBottomupFile, "j");
		File gitDir = new File(mgr.getArgStr("g"));
		File corpusDir = mgr.getLocalArgFile(conf.localCorpusFile, "c");
		File idIndexFile = new File(corpusDir, "idIndex.csv");
		File clustersFile = new File(localOutputDir, "clusters.csv");
		File priorityFile = new File(localOutputDir, "cluster_priority.json");
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
				Map<Integer, String> topics = getTopicStr(ldaFiles, conf);
				HierarchicalClusterGraph graph = HierarchicalClusterGraph.parseMergingMergedFile(mergingMergedFile, ldaFiles.documentPath, conf.hdfs);
				graph.setCentroidString(topics);
				graph.setDensity(measure);
				graph.dumpCSV(localOutputDir);
			} else {
				System.out.println("Found past result and reuse it. if you want to overwrite, then specify -ow");
			}

	        HierarchicalClusterPrioritizer p = new HierarchicalClusterPrioritizer(clustersFile);
			Map<Integer, List<String>> realId = getRealID(idIndexFile);
	        FileOutputStream outputStream = new FileOutputStream(priorityFile);
	        outputStream.write(createJson(p, realId, gitDir).toString(4).getBytes());
	        outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<Integer, String> getTopicStr(LDAHDFSFiles ldaFiles, NLPConf conf) throws IOException {
		Map<Integer, String> topics = new HashMap<Integer, String>();
		for (Entry<Integer, List<String>> e: new TopicReader(ldaFiles.dictionaryPath, ldaFiles.topicPath, conf.hdfs, 2).getTopics().entrySet()) {
			topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
		}
		return topics;
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

	public JSONObject createJson(HierarchicalClusterPrioritizer p, Map<Integer, List<String>> realId, File gitDir) throws IOException {
		Repository repo = new FileRepositoryBuilder().findGitDir(gitDir).build();
		RevWalk walk = new RevWalk(repo);
		JSONObject json = new OrderedJson();
		int i = 1;
		for (Entry<Double, HierarchicalCluster> e: p.getOrder().entrySet()) {
			HierarchicalCluster c = e.getValue();
			JSONObject cJson = new JSONObject();
			json.put("no. " + i, cJson);
			cJson.put("cluster ID", c.getID());
			cJson.put("parent ID", (c.getParent() != null ? c.getParent().getID(): "root"));

			JSONArray cArray = new JSONArray();
			cJson.put("child ID", cArray);
			cArray.put(c.getLeft() != null ? c.getLeft().getID(): "leaf");
			cArray.put(c.getRight() != null ? c.getRight().getID(): "leaf");

			cJson.put("size", c.size());

			JSONObject tJson = new JSONObject();
			cJson.put("centroid topic", tJson);
			for (Entry<String, Double> e2: c.getCentroid().entrySet()) {
				tJson.put(e2.getKey(), e2.getValue());
			}
			cJson.put("density", c.getDensity());
			cJson.put("score", e.getKey());

			JSONObject pJson = new JSONObject();
			cJson.put("points", pJson);
			for (int pointID: c.getPoints()) {
				JSONArray array = new JSONArray();
				List<String> l = realId.get(pointID);
				array.put(l);
				pJson.put(getSubject(repo, walk, l.get(0)), array);
			}
		}
		return json;
	}

	public String getSubject(Repository repo, RevWalk walk, String sha) throws IOException {
		return walk.parseCommit(repo.resolve(sha)).getShortMessage();
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
