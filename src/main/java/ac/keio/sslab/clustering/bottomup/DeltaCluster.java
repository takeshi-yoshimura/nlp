package ac.keio.sslab.clustering.bottomup;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class DeltaCluster {
	int ID;
	int size;
	double groupAverage;
	Map<Integer, Map<String, Double>> pointTopics;
	Map<Integer, String> pointSubjects;
	Map<Integer, List<String>> pointShas;

	public DeltaCluster(int ID, int size, double ga, Map<Integer, Map<String, Double>> pointTopics, Map<Integer, String> pointSubjects, Map<Integer, List<String>> pointShas) {
		this.ID = ID;
		this.size = size;
		this.groupAverage = ga;
		this.pointTopics = pointTopics;
		this.pointSubjects = pointSubjects;
		this.pointShas = pointShas;
	}

	protected void writeJson(SimpleJsonWriter json) throws IOException {
		json.writeNumberField("cluster ID", ID);
		json.writeNumberField("current size", size);
		json.writeNumberField("current group average", groupAverage);
		json.writeStartObject("new grouped patches");
		for (int pointID: pointShas.keySet()) {
			json.writeStartObject(Integer.toString(pointID));
			json.writeStringCollection("commit shas", pointShas.get(pointID));
			json.writeStringField("subject", pointSubjects.get(pointID));
			json.writeStringDoubleMap("topic", pointTopics.get(pointID));
			json.writeEndObject();
		}
	}

	public static DeltaCluster readJson(SimpleJsonReader json) throws IOException {
		Map<Integer, List<String>> pointShas = new HashMap<Integer, List<String>>();
		Map<Integer, String> pointSubjects = new HashMap<Integer, String>();
		Map<Integer, Map<String, Double>> pointTopics = new HashMap<Integer, Map<String, Double>>();

		int ID = json.readIntValue("cluster ID");
		int size = json.readIntValue("current size");
		double groupAverage = json.readDoubleValue("current group average");
		json.readStartObject("new grouped patches");
		while (!json.isCurrentTokenEndObject()) {
			String pointIDStr = json.getCurrentFieldName();
			json.readStartObject(pointIDStr);
			int pointID = Integer.parseInt(pointIDStr);
			pointShas.put(pointID, json.readStringCollection("commit shas"));
			pointSubjects.put(pointID, json.readStringField("subject"));
			pointTopics.put(pointID, json.readStringDoubleMap("topic"));
			json.readEndObject();
		}
		return new DeltaCluster(ID, size, groupAverage, pointTopics, pointSubjects, pointShas);
	}

	public String toPlainText(SimpleGitReader git) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("cluster ID: ").append(ID).append(", current size: ").append(size).append(", current group average").append(groupAverage).append('\n');
		for (int pointID: pointShas.keySet()) {
			sb.append("point ID: ").append(pointID).append(", commit shas:");
			for (Entry<String, Double> e: pointTopics.get(pointID).entrySet()) {
				sb.append(' ').append(e.getKey()).append(':').append(e.getValue());
			}
			sb.append("\ncommit shas:");
			for (String sha: pointShas.get(pointID)) {
				sb.append(' ').append(sha);
			}
			sb.append(git.showCommit(pointShas.get(pointID).get(0))).append('\n');
		}
		return sb.toString();
	}

	public int getID() {
		return ID;
	}

	public int getSize() {
		return size;
	}

	public double getGroupAverage() {
		return groupAverage;
	}
}
