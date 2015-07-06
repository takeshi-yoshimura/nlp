package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.TextProgressMonitor;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.util.FileUtils;

public class GitSetupJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

	@Override
	public String getJobName() {
		return NLPConf.gitSetupJobName;
	}

	@Override
	public String getJobDescription() {
		return "git clone or pull";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("u", "url", true, "URL of a cloned git repository (http or https only)");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		String jobID = args.get("j");
		File outputDir = new File(conf.localGitFile, jobID);
		TextProgressMonitor monitor = new TextProgressMonitor(new PrintWriter(System.out));
		if (!outputDir.exists() || args.containsKey("ow")) {
			//git clone
			if (!args.containsKey("u")) {
				System.err.println("Need to specify --url");
				return;
			}
			String url = args.get("u");
			try {
				if (!JobUtils.promptDeleteDirectory(outputDir, args.containsKey("force"))) {
					return;
				}
				conf.localGitFile.mkdirs();
		
				CloneCommand clone = Git.cloneRepository();
				clone.setBare(false);
				clone.setCloneAllBranches(true);
				clone.setDirectory(outputDir.getAbsoluteFile());
				clone.setURI(url);
				clone.setProgressMonitor(monitor);
				System.out.println("git clone " + url);
				Git git = clone.call();
				git.close();
			} catch (Exception e) {
				System.err.println("Cloning a git repository failed: " + e.toString());
				try {
					FileUtils.delete(conf.localGitFile, FileUtils.RECURSIVE);
				} catch (Exception e2) {
					System.err.println("Deleting " + conf.localGitFile.getAbsolutePath() + " failed. Delete them manually");
				}
			}
		} else {
			//git pull origin master
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			try {
				Repository repo = builder.findGitDir(outputDir).build();
				Git git = new Git(repo);
				ResetCommand reset = git.reset();
				reset.setRef(Constants.MASTER);
				reset.setMode(ResetType.HARD);
				System.out.println("git reset --hard master. Wait for a moment...");
				reset.call();

				PullCommand pull = git.pull();
				pull.setProgressMonitor(monitor);
				System.out.println("git pull");
				pull.call();
				git.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Pulling a git repository failed: " + e.toString());
			}
		}
	}

	@Override
	public void takeSnapshot() {
		/* do nothing */
	}

	@Override
	public void restoreSnapshot() {
		/* do nothing */
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
