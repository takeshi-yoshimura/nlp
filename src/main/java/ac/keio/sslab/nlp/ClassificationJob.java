package ac.keio.sslab.nlp;

import java.io.File;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.TopdownClassifier;

public class ClassificationJob implements NLPJob {

	@Override
	public String getJobName() {
		return "classification";
	}

	@Override
	public String getJobDescription() {
		return "extract similar patches from the topdown";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID for a bottomup job"));
		OptionGroup g6 = new OptionGroup();
		g6.addOption(new Option("ga", "groupAverage", true, "output clusters whose ga is lower than this value"));
		g.setRequired(true);
		g6.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g6);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File outputDir = new File(conf.finalOutputFile, "class/" + mgr.getArgStr("j"));
		File clustersFile = new File(conf.localBottomupFile + "/" + mgr.getArgStr("b"), "clusters.csv");

		System.out.println("Start at: " + new Date().toString());
		try {
			outputDir.getParentFile().mkdirs();
			System.out.println("loading " + clustersFile.getAbsolutePath());
			TopdownClassifier c = new TopdownClassifier(clustersFile);
			double ga = mgr.getArgOrDefault("ga", 1.0, Double.class);
			c.writeResultCSV(outputDir, ga);
	        System.out.println("Results: " + outputDir.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("End at:" + new Date().toString());
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
