package ac.keio.sslab.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class IncrementalClusterDumper {

	List<DeltaCluster> clusters;

	public IncrementalClusterDumper(List<DeltaCluster> clusters) {
		this.clusters = clusters;
	}

	public IncrementalClusterDumper(List<HierarchicalCluster> all, Map<Integer, Map<String, Double>> pointTopics, Map<Integer, List<String>> realIDs, SimpleGitReader git) throws IOException {
		clusters = new ArrayList<DeltaCluster>();
		Set<Integer> donePointID = new HashSet<Integer>();
		for (HierarchicalCluster h: all) {
			Map<Integer, String> pointSubject = new HashMap<Integer, String>();
			Map<Integer, List<String>> pointSha = new HashMap<Integer, List<String>>();
			Map<Integer, Map<String, Double>> pointTopic = new HashMap<Integer, Map<String, Double>>();
			for (int p: h.getPoints()) {
				if (donePointID.contains(p)) {
					continue;
				}
				donePointID.add(p);
				List<String> shas = realIDs.get(p);
				pointSha.put(p, shas);
				pointSubject.put(p, git.getSubject(shas.get(0)));
				pointTopic.put(p, pointTopics.get(p));
			}
			clusters.add(new DeltaCluster(h.ID, h.size(), h.getDensity(), pointTopics, pointSubject, pointSha));
		}
	}

	public int getFinalID() {
		return clusters.get(clusters.size() - 1).getID();
	}

	public int getFinalSize() {
		return clusters.get(clusters.size() - 1).getSize();
	}

	public double getFinalGroupAverage() {
		return clusters.get(clusters.size() - 1).getGroupAverage();
	}

	public void writeJson(SimpleJsonWriter json) throws IOException {
		json.writeStartObject("cluster");
		json.writeNumberField("final ID", getFinalID());
		json.writeNumberField("final size", getFinalSize());
		json.writeNumberField("final group average", getFinalGroupAverage());
		int i = 0;
		for (DeltaCluster cluster: clusters) {
			json.writeStartObject("iteration-" + i++);
			cluster.writeJson(json);
			json.writeEndObject();
		}
		json.writeEndObject();
	}

	public static IncrementalClusterDumper readJson(SimpleJsonReader json) throws IOException {
		json.readStartObject("cluster");
		json.readIntValue("final ID");
		json.readIntValue("final size");
		json.readDoubleValue("final group average");
		int i = 0;
		List<DeltaCluster> clusters = new ArrayList<DeltaCluster>();
		while (!json.isCurrentTokenEndObject()) {
			json.readStartObject("iteration-" + i++);
			clusters.add(DeltaCluster.readJson(json));
			json.readEndObject();
		}
		json.readEndObject();
		return new IncrementalClusterDumper(clusters);
	}

	public String toPlainText(SimpleGitReader git) throws Exception {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		sb.append("final cluster ID: ").append(getFinalID()).append('\n');
		sb.append("final size: ").append(getFinalSize()).append('\n');
		sb.append("final group average: ").append(getFinalGroupAverage()).append('\n');
		for (DeltaCluster cluster: clusters) {
			sb.append("===================iteration-" + i++ + "===================\n");
			sb.append(cluster.toPlainText(git));
		}
		return sb.toString();
	}
}
