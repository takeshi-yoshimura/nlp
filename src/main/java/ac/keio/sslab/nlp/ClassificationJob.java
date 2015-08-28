package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.PointCentricClusterWriter;

public class ClassificationJob implements NLPJob {

	@Override
	public String getJobName() {
		return "classification";
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
		opt.addOption("d", "density", false, "record density.csv for debugging purpose");
		opt.addOptionGroup(g);
		opt.addOptionGroup(g4);
		opt.addOptionGroup(g5);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File outputDir = new File(conf.finalOutputFile, "class/" + mgr.getArgStr("j"));
		File gitDir = new File(mgr.getArgStr("g"));
		File idIndexFile = new File(conf.localCorpusFile + "/" + mgr.getArgStr("c"), "idIndex.txt");
		File clustersFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "clusters.csv");
		File corpusIDIndexFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "corpusIDIndex.csv");
		File densityFile = new File(outputDir, "density.csv");

		try {
			outputDir.getParentFile().mkdirs();
			System.out.println("loading " + clustersFile.getAbsolutePath());
			PointCentricClusterWriter c = new PointCentricClusterWriter(clustersFile, corpusIDIndexFile, idIndexFile, gitDir);
			for (int pointID: c.getPointIDs()) {
				System.out.println("writing point ID = " + pointID);
				c.writeBestClusterJson(outputDir, pointID);
			}
	        System.out.println("Results: " + outputDir.getAbsolutePath());
			if (mgr.hasArg("d")) {
				c.writeAllDensityTrendCSV(densityFile);
		        System.out.println("Results: " + densityFile.getAbsolutePath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
