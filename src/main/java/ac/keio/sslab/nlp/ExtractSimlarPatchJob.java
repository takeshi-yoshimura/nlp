package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.PointCentoricCluster;

public class ExtractSimlarPatchJob implements NLPJob {

	@Override
	public String getJobName() {
		return "extractSimilarPatch";
	}

	@Override
	public String getJobDescription() {
		return "extract similar patches for each patch";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID for a bottomup job"));
		OptionGroup g4 = new OptionGroup();
		g4.addOption(new Option("c", "corpusID", true, "ID for a corpus job"));
		OptionGroup g5 = new OptionGroup();
		g5.addOption(new Option("g", "gitDir", true, "git directory"));
		g.setRequired(true);
		g4.setRequired(true);
		g5.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g4);
		opt.addOptionGroup(g5);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File localOutputDir = new File(conf.finalOutputFile, "summary/" + mgr.getArgStr("j"));
		File gitDir = new File(mgr.getArgStr("g"));
		File idIndexFile = new File(conf.localCorpusFile + "/" + mgr.getArgStr("c"), "idIndex.txt");
		File clustersFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "clusters.csv");
		File summaryFile = new File(localOutputDir, "summary.json");
		File densityFile = new File(localOutputDir, "density.csv");

		try {
			PointCentoricCluster c = new PointCentoricCluster(clustersFile);
			c.writeAllBestClustersJson(summaryFile, idIndexFile, gitDir);
			c.writeAllDensityTrendCSV(densityFile);
	        System.out.println("Results: " + summaryFile.getAbsolutePath());
	        System.out.println("Results: " + densityFile.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
