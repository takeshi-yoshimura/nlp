package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.DeleteBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class LinuxPatchJob implements NLPJob {

	NLPConf conf = new NLPConf();

	@Override
	public String getJobName() {
		return NLPConf.linuxPatchJobName;
	}

	@Override
	public String getJobDescription() {
		return "Patch linux for using clang analyzer";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("c", "corpusID", true, "ID of a corpus.");
		return options;
	}

	protected BufferedReader getJarReader(String path) {
		BufferedReader reader = null;
		InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path);
		if (stream != null) {
			reader = new BufferedReader(new InputStreamReader(stream));
		}
		return reader;
	}

	@Override
	public void run(Map<String, String> args) {
		String branchName = "nlp-" + args.get("j");
		if (!args.containsKey("c")) {
			System.err.println("Need to specify --corpusID");
			return;
		}
		String corpusID = args.get("c");
		
		JobManager mgr = new JobManager(NLPConf.gitCorpusJobName);
		Map<String, String> map = mgr.getJobIDArgs(corpusID);
		File gitFile = new File(conf.localGitFile, map.get("g"));
		if (map.containsKey("sl")) {
			System.err.println("--stableLinux job should be used only for LDA. Abort");
			return;
		}
		String sinceStr = "";
		if (map.containsKey("s")) {
			sinceStr = map.get("s");
		}
		String untilStr = "HEAD";
		if (map.containsKey("u")) {
			untilStr = map.get("u");
		}

		Repository repo = null;
		Git git = null;
		File versionFile;
		try {
			repo = new FileRepositoryBuilder().findGitDir(gitFile).build();
			git = new Git(repo);
			RevWalk walk = new RevWalk(repo);
			System.out.println("git show " + sinceStr);
			RevCommit sinceRev = walk.parseCommit(repo.resolve(sinceStr));
			System.out.println("git show " + untilStr);
			RevCommit untilRev = walk.parseCommit(repo.resolve(untilStr));

			String resetRev;
			if (sinceRev.getCommitTime() > untilRev.getCommitTime()) {
				resetRev = sinceStr;
			} else {
				resetRev = untilStr;
			}
			System.out.println("git reset --hard " + resetRev);
			git.reset().setMode(ResetType.HARD).setRef(resetRev).call();
			System.out.println("git describe");
			String tag = git.describe().call();
			versionFile = new File(NLPConf.linuxPatchFile, tag.substring(0, tag.lastIndexOf('.')));

			System.out.println(versionFile.toString());
			ListBranchCommand branchList = git.branchList();
			for (Ref e: branchList.call()) {
				if (e.getName().equals(branchName)) {
					System.out.println("git branch -d " + branchName);
					DeleteBranchCommand branch = git.branchDelete();
					branch.setBranchNames(branchName).call();
				}
			}
			System.out.println("git checkout -b " + branchName);
			CheckoutCommand checkout = git.checkout();
			checkout.setCreateBranch(true).setName(branchName).call();

			BufferedReader reader = getJarReader(versionFile.toString());
			if (reader != null) {
				File tmpFile = new File(conf.localTmpFile, "patch");
				tmpFile.deleteOnExit();
				PrintWriter pw = JobUtils.getPrintWriter(tmpFile);
				String line = reader.readLine();
				while (line != null) {
					pw.println(line);
					line = reader.readLine();
				}
				pw.close();
				System.out.println("patch -p 1 < " + tmpFile.getAbsolutePath());
				JobUtils.runGNUPatch(tmpFile, gitFile);
			}

			AddCommand add = git.add();
			add.addFilepattern(".").call();
			CommitCommand commit = git.commit();
			commit.setMessage("Analyze Linux kernel").setAuthor(new PersonIdent("Takeshi Yoshimura", "yos@sslab.ics.keio.ac.jp")).setAll(true).call();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Patching " + gitFile.getAbsolutePath() + " failed" + e.toString());
		}
		if (git != null) {
			git.close();
		}
		if (repo != null) {
			repo.close();
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
