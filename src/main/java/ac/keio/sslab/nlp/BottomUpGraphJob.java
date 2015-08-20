package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.HierarchicalClusterGraphViewer;

public class BottomUpGraphJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomupGraph";
	}

	@Override
	public String getJobDescription() {
		return "bottomup clustering graph viewer";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID for a bottomup job"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOption("s", "startID", true, "starting cluster ID for graph (default: root)");
		opt.addOption("n", "numHierarchy", true, "number of Hierarchy to view (default: 5)");
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File localOutputDir = mgr.getLocalArgFile(conf.localBottomupFile, "j");
		File clustersFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "clusters.csv");

		try {
			HierarchicalClusterGraphViewer v= new HierarchicalClusterGraphViewer(clustersFile);
			v.dumpDot(localOutputDir, mgr.getArgOrDefault("s", v.getRootID(), Integer.class), mgr.getArgOrDefault("n", 5, Integer.class));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
