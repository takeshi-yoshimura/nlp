package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
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
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

import ac.keio.sslab.clustering.bottomup.ClusterScore;
import ac.keio.sslab.clustering.bottomup.HierarchicalCluster;

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
	        ClusterScore p = new ClusterScore(clustersFile);
			Map<Integer, List<String>> realId = getRealID(idIndexFile);
			createJson(p, realId, summaryFile, gitDir);
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

	public void createJson(ClusterScore p, Map<Integer, List<String>> realId, File output, File gitDir) throws IOException {
		Repository repo = new FileRepositoryBuilder().findGitDir(gitDir).build();
		RevWalk walk = new RevWalk(repo);
		ObjectMapper mapper = new ObjectMapper().configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		JsonGenerator json = mapper.getJsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);
		json.setPrettyPrinter(new DefaultPrettyPrinter());
		json.writeStartObject();
		int i = 1;
		for (Entry<Double, HierarchicalCluster> e: p.getOrder().entrySet()) {
			HierarchicalCluster c = e.getValue();
			json.writeFieldName("No. " + i++);
			json.writeStartObject();
			json.writeNumberField("cluster ID", c.getID());
			if (c.getParent() != null) {
				json.writeNumberField("parent ID", c.getParent().getID());
			}

			if (c.getLeft() != null) {
				json.writeFieldName("child ID");
				json.writeStartArray();
				json.writeNumber(c.getLeft().getID());
				json.writeNumber(c.getRight().getID());
				json.writeEndArray();
			}

			json.writeNumberField("size", c.size());
			json.writeFieldName("centroid topic");
			json.writeStartObject();
			for (Entry<String, Double> e2: c.getCentroid().entrySet()) {
				json.writeNumberField(e2.getKey(), e2.getValue());
			}
			json.writeEndObject();
			json.writeNumberField("density", c.getDensity());
			json.writeNumberField("score", e.getKey());

			json.writeFieldName("points");
			json.writeStartObject();
			for (int pointID: c.getPoints()) {
				List<String> l = realId.get(pointID);
				json.writeFieldName(getSubject(repo, walk, l.get(0)));
				json.writeStartArray();
				for (String s: l) {
					json.writeString(s);
				}
				json.writeEndArray();
			}
			json.writeEndObject();
			json.writeEndObject();
		}
	}

	public String getSubject(Repository repo, RevWalk walk, String sha) throws IOException {
		return walk.parseCommit(repo.resolve(sha)).getShortMessage();
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
