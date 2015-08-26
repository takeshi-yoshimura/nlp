package ac.keio.sslab.clustering.bottomup;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class ClusterMetrics {
	int ID;
	int size;
	double groupAverage;
	Map<String, Double> centroidTopic;
	Map<Integer, String> pointSubjects;
	Map<Integer, List<String>> pointShas;

	public ClusterMetrics(int ID, int size, double ga, Map<String, Double> c, Map<Integer, String> pointSubjects, Map<Integer, List<String>> pointShas) {
		this.ID = ID;
		this.size = size;
		this.groupAverage = ga;
		this.centroidTopic = c;
		this.pointSubjects = pointSubjects;
		this.pointShas = pointShas;
	}

	public ClusterMetrics(HierarchicalCluster c, Map<Integer, List<String>> realIDs, SimpleGitReader git) throws IOException {
		this.ID = c.getID();
		this.size = c.size();
		this.groupAverage = c.getDensity();
		this.centroidTopic = c.getCentroid();
		this.pointSubjects = new HashMap<Integer, String>();
		this.pointShas = new HashMap<Integer, List<String>>();
		for (int p: c.getPoints()) {
			List<String> shas = realIDs.get(p);
			this.pointShas.put(p, shas);
			this.pointSubjects.put(p, git.getSubject(shas.get(0)));
		}
	}

	public void writeJson(SimpleJsonWriter json) throws IOException {
		json.writeStartObject("cluster");
		json.writeNumberField("ID", ID);
		json.writeNumberField("size (deduplicated)", size);
		json.writeNumberField("group average", groupAverage);
		json.writeStringDoubleMap("centroid topic", centroidTopic);
		json.writeStartObject("grouped patches");
		for (int pointID: pointShas.keySet()) {
			json.writeStartObject(Integer.toString(pointID));
			json.writeStringField("subject", pointSubjects.get(pointID));
			json.writeStringCollection("commit shas", pointShas.get(pointID));
			json.writeEndObject();
		}
		json.writeEndObject();
		json.writeEndObject();
	}

	public String toPlainText() {
		StringBuilder sb = new StringBuilder();
		sb.append("ID: ").append(ID).append('\n');
		sb.append("size (deduplicated): ").append(size).append('\n');
		sb.append("group average: ").append(groupAverage).append('\n');
		sb.append("centroid topic:\n");
		for (Entry<String, Double> e: centroidTopic.entrySet()) {
			sb.append(e.getKey()).append(':').append(e.getValue()).append('\n');
		}
		sb.append("grouped patches:\n");
		for (int pointID: pointShas.keySet()) {
			sb.append("point ID:").append(pointID).append(":\n");
			sb.append("\tsubject: ").append(pointSubjects.get(pointID)).append('\n');
			sb.append("\tcommit shas:");
			for (String sha: pointShas.get(pointID)) {
				sb.append(' ').append(sha);
			}
			sb.append("\n\n");
		}
		return sb.toString();
	}

	public static ClusterMetrics readJson(SimpleJsonReader json) throws IOException {
		json.readStartObject("cluster");
		int ID = json.readIntValue("ID");
		int size = json.readIntValue("size (deduplicated)");
		double groupAverage = json.readDoubleValue("group average");
		Map<String, Double> centroidTopic = json.readStringDoubleMap("centroid topic");
		json.readStartObject("grouped patches");
		Map<Integer, List<String>> pointShas = new HashMap<Integer, List<String>>();
		Map<Integer, String> pointSubjects = new HashMap<Integer, String>();
		while (!json.isCurrentTokenEndObject()) {
			String pointIDStr = json.getCurrentFieldName();
			json.readStartObject(pointIDStr);
			int pointID = Integer.parseInt(pointIDStr);
			pointSubjects.put(pointID, json.readStringField("subject"));
			pointShas.put(pointID, json.readStringCollection("commit shas"));
			json.readEndObject();
		}
		json.readEndObject();
		return new ClusterMetrics(ID, size, groupAverage, centroidTopic, pointSubjects, pointShas);
	}

	public Map<Integer, List<String>> getPointShas() {
		return pointShas;
	}
}