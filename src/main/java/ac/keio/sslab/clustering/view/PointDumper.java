package ac.keio.sslab.clustering.view;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class PointDumper {
	int pointID;
	String subject;
	List<Date> dates;
	List<String> versions;
	List<String> shas;
	List<String> files;
	Map<String, Double> topic;
	IncrementalClusterDumper c;

	public PointDumper(int pointID, String subject, List<Date> dates, List<String> versions, List<String> shas, List<String> files, Map<String, Double> topic, IncrementalClusterDumper c) {
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
		json.writeStringCollection("commits", shas);
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
		sb.append("\nversion:");
		for (String version: versions) {
			sb.append(' ').append(version);
		}
		sb.append("\ncommits:");
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

	public static PointDumper readJson(File inputDir, int pointID) throws Exception {
		File f = new File(inputDir, (pointID / 10000) + "/" + pointID + ".json");
		if (!f.exists()) {
			return null;
		}
		SimpleJsonReader json = new SimpleJsonReader(f);
		json.readIntValue("ID");
		String subject = json.readStringField("subject");
		List<Date> dates = json.readDateCollection("date");
		List<String> versions = json.readStringCollection("version");
		List<String> shas = json.readStringCollection("commits");
		List<String> files = json.readStringCollection("files");
		Map<String, Double> topic = json.readStringDoubleMap("topics");
		IncrementalClusterDumper c = IncrementalClusterDumper.readJson(json);
		json.close();
		return new PointDumper(pointID, subject, dates, versions, shas, files, topic, c);
	}

	public IncrementalClusterDumper getClusterMetrics() {
		return c;
	}

	public int getPointID() {
		return pointID;
	}

	public List<String> getShas() {
		return shas;
	}
}
