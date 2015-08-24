package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import ac.keio.sslab.nlp.JobUtils;

public class ClusterGraphDumper {

	HierarchicalCluster root;
	List<HierarchicalCluster> clusters;

	public ClusterGraphDumper(File outputFile) throws IOException {
		ClusterGraph graph = ClusterGraph.parseResult(outputFile);
		root = graph.getRoot();
		clusters = graph.getClusters();
	}

	public int getRootID() {
		return root.getID();
	}

	public void dumpDotFromRoot(File outputDir, int numHierarchy) throws IOException {
		dumpDot(outputDir, root.getID(), numHierarchy);
	}

	public void dumpPDFFromRoot(File outputDir, int numHierarchy, boolean removeDotFile) throws IOException {
		dumpPDF(outputDir, root.getID(), numHierarchy, removeDotFile);
	}

	public File dumpDot(File outputDir, int startID, int numHierarchy) throws IOException {
		String dendName = "dend_" + startID + "_" + numHierarchy;
		File outputFile = new File(outputDir, dendName + ".dot");
		PrintWriter writer = JobUtils.getPrintWriter(outputFile);
		writer.println("digraph " + dendName + " {");
		HierarchicalCluster current = findID(startID);
		dumpDotTraverse(writer, current, numHierarchy);
		writer.println("}");
		writer.close();

		return outputFile;
	}

	public void dumpPDF(File outputDir, int startID, int numHierarchy, boolean removeDotFile) throws IOException {
		File dotFile = dumpDot(outputDir, startID, numHierarchy);
		ProcessBuilder pb = new ProcessBuilder();
		List<String> cmd = new ArrayList<String>();
		cmd.add("dot");
		cmd.add("-Tpdf");
		cmd.add(dotFile.getAbsolutePath());
		cmd.add("-o");
		cmd.add(new File(outputDir, dotFile.getName().split("\\.")[0] + ".pdf").getAbsolutePath());
		pb.command(cmd);
		try {
			int ret = pb.start().waitFor();
			if (ret == 0 && removeDotFile) {
				dotFile.delete();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected HierarchicalCluster findID(int ID) {
		for (HierarchicalCluster c: clusters) {
			if (c.getID() == ID) {
				return c;
			}
		}
		return null;
	}

	private void dumpDotTraverse(PrintWriter writer, HierarchicalCluster current, int numHierarchy) {
		if (current == null || numHierarchy <= 0) {
			return;
		}

		writer.print("\tC" + current.getID() + " [shape = record, label=\"{{C" + current.getID() + " | N(p) = " + current.points.size() + "}");
		writer.print("| density = " + String.format("%1$3f", current.density));
		for (Entry<String, Double> e: current.getCentroid().entrySet()) {
			writer.print("|" + e.getKey() + ": " + String.format("%1$3f", e.getValue()));
		}
		writer.println("}\"];");

		if (current.leftC != null) {
			writer.println("\tC" + current.getID() + " -> " + "C" + current.leftC.getID() + ";");
		}
		if (current.rightC != null) {
			writer.println("\tC" + current.getID() + " -> " + "C" + current.rightC.getID() + ";");
		}
		writer.println();

		dumpDotTraverse(writer, current.leftC, numHierarchy - 1);
		dumpDotTraverse(writer, current.rightC, numHierarchy - 1);
	}
}
