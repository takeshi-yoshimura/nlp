package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonReader;

public class PointCentricClusterReader {

	File summaryFile;
	SimpleGitReader git;

	public PointCentricClusterReader(File summaryFile, File gitDir) throws IOException {
		this.summaryFile = summaryFile;
		git = new SimpleGitReader(gitDir);
	}

	public void getFullInfo(String pointID, OutputStream writer) throws Exception {
		SimpleJsonReader reader = new SimpleJsonReader(summaryFile);
		PointMetrics p = null;
		while (p == null && !reader.isCurrentTokenEndObject()) {
			p = PointMetrics.readIfMatchingPointID(reader, pointID);
		}
		reader.close();
		if (p == null) {
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("Detail of similar patches for point ID = ").append(p.getPointID());
		sb.append("\n=========================Cluster info=========================\n");
		sb.append(p.getClusterMetrics().toPlainText());
		writer.write(sb.toString().getBytes());
		writer.flush();

		Set<Integer> points = p.getClusteredPoints();

		reader = new SimpleJsonReader(summaryFile);
		List<PointMetrics> pointMetrics = new ArrayList<PointMetrics>();
		while (!reader.isCurrentTokenEndObject()) {
			p = PointMetrics.readJson(reader);
			if (points.contains(p.getPointID())) {
				pointMetrics.add(p);
			}
		}
		reader.close();

		for (PointMetrics pm: pointMetrics) {
			sb.setLength(0);
			sb.append("======================Clustered point info======================\n");
			sb.append(pm.toPlainText());
			sb.append("----------------------------git log----------------------------\n");
			sb.append(git.showCommit(pm.getShas().get(0))).append('\n');
			writer.write(sb.toString().getBytes());
			writer.flush();
		}
		writer.close();
	}
}
