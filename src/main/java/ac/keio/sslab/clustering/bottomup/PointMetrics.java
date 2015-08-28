package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class PointMetrics {
	int pointID;
	String subject;
	List<Date> dates;
	List<String> versions;
	List<String> shas;
	List<String> files;
	Map<String, Double> topic;
	ClusterMetrics c;

	public PointMetrics(int pointID, String subject, List<Date> dates, List<String> versions, List<String> shas, List<String> files, Map<String, Double> topic, ClusterMetrics c) {
		this.pointID = pointID;
		this.subject = subject;
		this.dates = dates;
		this.versions = versions;
		this.shas = shas;
		this.files = files;
		this.topic = topic;
		this.c = c;
	}

	public void writeJson(File outputDir) throws Exception {
		File f = new File(outputDir, (pointID / 10000) + "/" + pointID + ".json");
		f.getParentFile().mkdirs();
		SimpleJsonWriter json = new SimpleJsonWriter(f);
		json.writeNumberField("ID", pointID);
		json.writeStringField("subject", subject);
		json.writeDateCollection("date", dates);
		json.writeStringCollection("version", versions);
		json.writeStringCollection("commit shas", shas);
		json.writeStringCollection("files", files);
		json.writeStringDoubleMap("topics", topic);
		c.writeJson(json);
		json.close();
	}

	public String toPlainText() {
		StringBuilder sb = new StringBuilder();
		sb.append("ID: ").append(pointID).append('\n');
		sb.append("subject: ").append(subject).append('\n');
		sb.append("date:");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		for (Date date: dates) {
			sb.append(' ').append(sdf.format(date));
		}
		sb.append("version:");
		for (String version: versions) {
			sb.append(' ').append(version);
		}
		sb.append("\ncommit shas:");
		for (String sha: shas) {
			sb.append(' ').append(sha);
		}
		sb.append("\nfiles:");
		for (String file: files) {
			sb.append(' ').append(file);
		}
		sb.append("\ntopics:\n");
		for (Entry<String, Double> e: topic.entrySet()) {
			sb.append(e.getKey()).append(':').append(e.getValue()).append('\n');
		}
		return sb.toString();
	}

	public static PointMetrics readJson(File inputDir, int pointID) throws Exception {
		File f = new File(inputDir, (pointID / 10000) + "/" + pointID + ".json");
		SimpleJsonReader json = new SimpleJsonReader(f);
		json.readIntValue("pointID");
		String subject = json.readStringField("subject");
		List<Date> dates = json.readDateCollection("date");
		List<String> versions = json.readStringCollection("version");
		List<String> shas = json.readStringCollection("commit shas");
		List<String> files = json.readStringCollection("files");
		Map<String, Double> topic = json.readStringDoubleMap("topics");
		ClusterMetrics c = ClusterMetrics.readJson(json);
		json.close();
		return new PointMetrics(pointID, subject, dates, versions, shas, files, topic, c);
	}

	public ClusterMetrics getClusterMetrics() {
		return c;
	}

	public int getPointID() {
		return pointID;
	}

	public List<String> getShas() {
		return shas;
	}
}
