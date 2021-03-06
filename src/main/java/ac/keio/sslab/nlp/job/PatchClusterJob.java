package ac.keio.sslab.nlp.job;

import java.io.File;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.analytics.PatchIDResolver;
import ac.keio.sslab.analytics.PointDumper;
import ac.keio.sslab.clustering.bottomup.BottomupClassifier;
import ac.keio.sslab.utils.SimpleGitReader;

public class PatchClusterJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "bottomup.simpatch";
	}

	@Override
	public String getShortJobName() {
		return "pc";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new BottomUpJob();
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, getJobName());
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "show similar patches for a patch";
	}

	@Override
	public Options getOptions() {
		Options opt = new Options();
		opt.addOption("p", "pointID", true, "point ID generated by bottomup job");
		opt.addOption("o", "orignalID", true, "original patch ID");
		return opt;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		JobManager bottomupMgr = pMgr;
		JobManager corpusMgr = bottomupMgr.getParentJobManager().getParentJobManager();
		corpusMgr.lock();
		File bottomupDir = bottomupMgr.getLocalOutputDir();
		File corpusDir = corpusMgr.getLocalOutputDir();
		if (!corpusMgr.hasArg("g")) {
			throw new Exception("Not implemented for corpora from corpus.text job");
		}
		File gitDir = new File(corpusMgr.getArgStr("g"));
		File classDir = mgr.getLocalOutputDir();

		int pointID = mgr.getArgOrDefault("p", -1, Integer.class);
		String patchID = mgr.getArgOrDefault("o", null, String.class);
		if ((pointID == -1 && patchID == null) || (pointID != -1 && patchID != null)) {
			System.err.println("Specify either -p or -o");
			return;
		}

		if (patchID != null) {
			pointID = PatchIDResolver.toPointID(patchID, corpusDir, bottomupDir);
			System.out.println("Use point ID = " + pointID + " for " + patchID);
		}

		SimpleGitReader git = new SimpleGitReader(gitDir);
		PointDumper p = PointDumper.readJson(classDir, pointID);
		if (p == null) {
			System.out.println("Seems like we could not find the file for " + pointID + " in " + classDir.getAbsolutePath());
			System.out.println("Extract clusters for the patch. wait a moment...");
			BottomupClassifier c = new BottomupClassifier(bottomupDir, corpusDir, gitDir);
			if (!c.getPointIDs().contains(pointID)) {
				System.err.println("Invalid point ID: " + pointID);
				return;
			}
			c.writeBestClusterJson(classDir, pointID);
			System.out.println("Finished! Retry read json for " + pointID);
			p = PointDumper.readJson(classDir, pointID);
		}
		System.out.println(p.toPlainText());
		System.out.println(p.getClusterMetrics().toPlainText(git));
		corpusMgr.unLock();
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
