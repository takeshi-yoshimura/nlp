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
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import ac.keio.sslab.clustering.bottomup.HierarchicalCluster;
import ac.keio.sslab.clustering.bottomup.HierarchicalClusterPrioritizer;
import ac.keio.sslab.utils.OrderedJson;

public class ClusteringResultJob implements NLPJob {

	@Override
	public String getJobName() {
		return "clusteringResult";
	}

	@Override
	public String getJobDescription() {
		return "summary of LDA and bottomup jobs";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID for a bottomup job"));
		OptionGroup g4 = new OptionGroup();
		g4.addOption(new Option("c", "corpusID", true, "ID for a corpus job"));
		OptionGroup g5 = new OptionGroup();
		g5.addOption(new Option("g", "gitDir", true, "git directory"));
		g.setRequired(true);
		g4.setRequired(true);
		g5.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g4);
		opt.addOptionGroup(g5);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File localOutputDir = mgr.getLocalArgFile(conf.localBottomupFile, "j");
		File gitDir = new File(mgr.getArgStr("g"));
		File idIndexFile = new File(conf.localCorpusFile + "/" + mgr.getArgStr("c"), "idIndex.txt");
		File clustersFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "clusters.csv");
		File summaryFile = new File(localOutputDir, "summary.json");

		try {
	        HierarchicalClusterPrioritizer p = new HierarchicalClusterPrioritizer(clustersFile);
			Map<Integer, List<String>> realId = getRealID(idIndexFile);
	        FileOutputStream outputStream = new FileOutputStream(summaryFile);
	        outputStream.write(createJson(p, realId, gitDir).toString(4).getBytes());
	        outputStream.close();
	        System.out.println("Results: " + summaryFile.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Map<Integer, List<String>> getRealID(File input) throws IOException {
		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String [] s = line.split("\t\t");
			String [] s2 = s[1].split(",");
			List<String> a = new ArrayList<String>();
			for (int i = 0; i < s2.length; i++) {
				if (s2[i].isEmpty()) {
					continue;
				}
				a.add(s2[i]);
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
			json.put("no. " + i++, cJson);
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
				List<String> l = realId.get(pointID);
				pJson.put(getSubject(repo, walk, l.get(0)), l);
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
