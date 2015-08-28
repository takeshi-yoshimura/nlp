package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.PointMetrics;
import ac.keio.sslab.utils.SimpleGitReader;

public class PatchMetricsJob implements NLPJob {

	@Override
	public String getJobName() {
		return "patchMetrics";
	}

	@Override
	public String getJobDescription() {
		return "print full metrics for a patch to standard output";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("cls", "classificationID", true, "ID for a classification job"));
		OptionGroup g5 = new OptionGroup();
		g5.addOption(new Option("g", "gitDir", true, "git directory"));
		g.setRequired(true);
		g5.setRequired(true);

		Options opt = new Options();
		opt.addOption("p", "patchID", true, "patch ID in idIndex generated by corpus job");
		opt.addOption("s", "sha", true, "commit sha");
		opt.addOption("c", "corpusID", true, "ID for corpus job if specifying -s");
		opt.addOptionGroup(g);
		opt.addOptionGroup(g5);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		File gitDir = new File(mgr.getArgStr("g"));
		File classDir = new File(conf.finalOutputFile, "class/" + mgr.getArgStr("cls"));
		int patchID = mgr.getArgOrDefault("p", -1, Integer.class);
		String sha = mgr.getArgOrDefault("s", null, String.class);
		String corpusID = mgr.getArgOrDefault("c", null, String.class);
		if ((patchID == -1 && sha == null) || (patchID != -1 && sha != null)) {
			System.err.println("Specify either -p or -s");
			return;
		} else if (sha != null && corpusID == null) {
			System.err.println("Specify -c if you use -c");
			return;
		}

		try {
			if (sha != null) {
				File idIndex = new File(conf.localCorpusFile, corpusID + "/idIndex.txt");
				BufferedReader br = new BufferedReader(new FileReader(idIndex));
				String line = null;
				boolean found = false;
				while ((line = br.readLine()) != null) {
					if (!line.contains(sha)) {
						continue;
					}
					patchID = Integer.parseInt(line.split("\t")[0]);
					found = true;
					break;
				}
				br.close();
				if (!found) {
					System.err.println("Could not find sha " + sha + " in " + idIndex.getAbsolutePath());
					return;
				}
			}

			SimpleGitReader git = new SimpleGitReader(gitDir);
			PointMetrics p = PointMetrics.readJson(classDir, patchID);
			System.out.println(p.toPlainText());
			System.out.println(p.getClusterMetrics().toPlainText(git));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
