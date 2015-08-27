package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import ac.keio.sslab.utils.SimpleGitReader;

public class PointCentricClusterReader {

	File classDir;
	SimpleGitReader git;

	public PointCentricClusterReader(File classDir, File gitDir) throws IOException {
		this.classDir = classDir;
		git = new SimpleGitReader(gitDir);
	}

	public void getFullInfo(int pointID, OutputStream writer) throws Exception {
		PointMetrics p = PointMetrics.readJson(new File(classDir, (pointID / 10000) + "/" + pointID + ".json"));
		StringBuilder sb = new StringBuilder();
		sb.append("Detail of similar patches for point ID = ").append(p.getPointID());
		sb.append("\n=========================Cluster info=========================\n");
		sb.append(p.getClusterMetrics().toPlainText());
		writer.write(sb.toString().getBytes());
		writer.flush();

		for (int pID: p.getClusteredPoints()) {
			PointMetrics pm = PointMetrics.readJson(new File(classDir, (pID / 10000) + "/" + pID + ".json"));
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
