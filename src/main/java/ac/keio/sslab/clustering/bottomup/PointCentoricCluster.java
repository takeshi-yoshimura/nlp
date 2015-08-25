package ac.keio.sslab.clustering.bottomup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.SimpleGitReader;

public class PointCentoricCluster {

	List<HierarchicalCluster> singletons;

	public PointCentoricCluster(File clustersFile) throws IOException {
		singletons = ClusterGraph.parseResult(clustersFile).getSingletons();
	}

	public List<Integer> getPointIDs() {
		List<Integer> points = new ArrayList<Integer>();
		for (HierarchicalCluster c: singletons) {
			points.add(c.getPoints().get(0));
		}
		return points;
	}

	public void writeAllDensityTrendCSV(File output) throws IOException {
		PrintWriter writer = JobUtils.getPrintWriter(output);
		StringBuilder sb = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (HierarchicalCluster c: singletons) {
			TreeMap<Integer, Double> trend = getDensityTrend(c.getPoints().get(0));
			sb.setLength(0);
			sb2.setLength(0);
			sb.append('#').append(c.getPoints().get(0)).append('\n');
			for (Entry<Integer, Double> e: trend.entrySet()) {
				sb.append(e.getKey()).append(',');
				sb2.append(e.getValue()).append(',');
			}
			sb.setLength(sb.length() - 1);
			sb2.setLength(sb2.length() - 1);
			writer.println(sb.toString());
			writer.println(sb2.toString());
			writer.flush();
		}
		writer.close();
	}

	public TreeMap<Integer, Double> getDensityTrend(int pointID) {
		TreeMap<Integer, Double> trend = new TreeMap<Integer, Double>();
		HierarchicalCluster current = singletons.get(pointID);
		while (current != null) {
			trend.put(current.size(), current.getDensity());
			current = current.getParent();
		}
		return trend;
	}

	public void writeAllBestClustersJson(File output, File idIndexFile, File gitDir) throws Exception {
		Map<Integer, List<String>> realIDs = getRealIDs(idIndexFile);
		SimpleGitReader git = new SimpleGitReader(gitDir);
		ObjectMapper mapper = new ObjectMapper().configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		JsonGenerator json = mapper.getJsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);
		json.setPrettyPrinter(new DefaultPrettyPrinter());
		json.writeStartObject();
		for (HierarchicalCluster singleton: singletons) {
			int pointID = singleton.getPoints().get(0);
			HierarchicalCluster c = getBestCluster(pointID);
			json.writeNumber(pointID);
			json.writeStartObject();
			json.writeStringField("subject", git.getSubject(realIDs.get(pointID).get(0)));
			json.writeStringField("date", git.getCommitDateString(realIDs.get(pointID).get(0)));
			json.writeStringField("version", git.getLatestTag(realIDs.get(pointID).get(0)));

			Set<String> files = new HashSet<String>();
			json.writeFieldName("commit shas");
			json.writeStartArray();
			for (String sha: realIDs.get(pointID)) {
				json.writeString(sha);
				files.addAll(git.getFiles(sha));
			}
			json.writeEndArray();

			json.writeFieldName("files");
			json.writeStartArray();
			for (String file: files) {
				json.writeString(file);
			}
			json.writeEndArray();

			json.writeFieldName("topics");
			json.writeStartObject();
			for (Entry<String, Double> e2: singleton.getCentroid().entrySet()) {
				json.writeNumberField(e2.getKey(), e2.getValue());
			}
			json.writeEndObject();

			json.writeString("cluster");
			json.writeStartObject();
			json.writeNumberField("ID", c.getID());
			json.writeNumberField("size", c.size());
			json.writeNumberField("group average", c.getDensity());

			json.writeFieldName("centroid topic");
			json.writeStartObject();
			for (Entry<String, Double> e2: c.getCentroid().entrySet()) {
				json.writeNumberField(e2.getKey(), e2.getValue());
			}
			json.writeEndObject();

			json.writeFieldName("grouped patches");
			json.writeStartObject();
			for (int p: c.getPoints()) {
				List<String> l = realIDs.get(p);
				json.writeFieldName(git.getSubject(l.get(0)));
				json.writeStartArray();
				for (String s: l) {
					json.writeString(s);
				}
				json.writeEndArray();
			}
			json.writeEndObject();

			json.writeEndObject();

			json.writeEndObject();
		}
		json.writeEndObject();
	}

	public Map<Integer, List<String>> getRealIDs(File idIndexFile) throws IOException {
		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(idIndexFile));
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

	public HierarchicalCluster getBestCluster(int pointID) {
		TreeMap<Double, HierarchicalCluster> scores = new TreeMap<Double, HierarchicalCluster>();
		HierarchicalCluster current = singletons.get(pointID);
		HierarchicalCluster parent = current.getParent();

		while (current != null && parent != null) {
			scores.put(parent.getDensity() - current.getDensity(), current);
			current = parent;
			parent = parent.getParent();
		}

		return scores.lastEntry().getValue();
	}
}
