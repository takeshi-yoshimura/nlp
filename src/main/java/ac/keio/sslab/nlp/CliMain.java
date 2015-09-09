package ac.keio.sslab.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.el.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import ac.keio.sslab.statistics.CompareWithManualJob;
import ac.keio.sslab.statistics.LoadBugResultJob;

public class CliMain {
	protected Map<String, NLPJob> jobs;
	protected NLPConf conf;

	public CliMain(String confFile) {
		jobs = new TreeMap<String, NLPJob>();
		this.conf = NLPConf.getInstance();
		this.conf.loadConfFile(confFile);
		this.jobs.put("help", null);
		this.jobs.put("bg", null);
	}

	public CliMain(List<NLPJob> jobs, String confFile) {
		this.jobs = new TreeMap<String, NLPJob>();
		this.conf = NLPConf.getInstance();
		this.conf.loadConfFile(confFile);
		for (NLPJob job: jobs) {
			this.jobs.put(job.getAlgorithmName(), job);
		}
		this.jobs.put("help", null);
		this.jobs.put("fg", null);
		this.jobs.put("list", null);
	}

	public void showJobs() {
		System.out.println("Available commands:");
		for (Entry<String, NLPJob> e: jobs.entrySet()) {
			if (e.getValue() != null) {
				System.out.println(e.getValue().getAlgorithmName() + "\t" + e.getValue().getAlgorithmDescription());
			}
		}
		System.out.println("list\tShow past job IDs");
		System.out.println("help\tShow available commands and their descriptions");
	}

	public void listJobs(String jobName) throws IOException {
		NLPConf conf = NLPConf.getInstance();
		File argFile = new File(conf.localArgFile, jobName);
		argFile.getParentFile().mkdirs();
		if (!argFile.exists()) {
			System.out.println("Job group " + jobName + " has never been invoked or not found");
		} else {
			FileInputStream inputStream = new FileInputStream(argFile);
			JSONObject jobJson = new JSONObject(IOUtils.toString(inputStream));
			StringBuilder sb = new StringBuilder();
			for (Object jobID: jobJson.keySet()) {
				sb.setLength(0);
				sb.append((String)jobID).append(":");
				JSONObject argObj = jobJson.getJSONObject((String)jobID);
				for (Object arg: argObj.keySet()) {
					sb.append(" -").append((String)arg).append(" ").append(argObj.get((String)arg));
				}
				System.out.println(sb.toString());
			}
	        inputStream.close();
		}
	}

	public void forkProcess(NLPJob job, String jobID, String [] args) throws Exception {
		ProcessBuilder pb = new ProcessBuilder();
		List<String> cmd = new ArrayList<String>();
		cmd.add("nohup");
		cmd.add(conf.runPath);
		cmd.add("fg"); //a hidden job name
		cmd.add(job.getAlgorithmName());
		for (String arg: args) {
			cmd.add(arg);
		}
		for (String c: cmd) {
			System.out.print(c + " ");
		}
		pb.command(cmd);
		File outputDir = new File(conf.localLogFile, job.getAlgorithmName() + "/" + jobID);
		outputDir.mkdirs();
		File stdoutFile = new File(outputDir, "stdout");
		File stderrFile = new File(outputDir, "stderr");
		pb.redirectOutput(stdoutFile);
		pb.redirectError(stderrFile);
		pb.redirectInput(new File("/dev/null"));

		System.out.println("The process for a job remains running in a background");
		System.out.println("Stdout: " + stdoutFile.getAbsolutePath());
		System.out.println("Stderr: " + stderrFile.getAbsolutePath());
		pb.start(); //restart this class from calling main()
	}

	public void run(String [] args) throws Exception {
		if (args.length == 0 || !jobs.containsKey(args[0]) || args[0].equals("help")) {
			showJobs();
			return;
		}
		NLPJob job;
		String [] newArgs;
		boolean runInBackground;
		if (args[0].equals("fg")) {
			if (args.length <= 2) {
				showJobs();
				return;
			}
			job = jobs.get(args[1]);
			newArgs = new String[args.length - 2];
			System.arraycopy(args, 2, newArgs, 0, args.length - 2);
			runInBackground = false;
		} else if (args[0].equals("list")) {
			listJobs(args[1]);
			return;
		} else {
			job = jobs.get(args[0]);
			newArgs = new String[args.length - 1];
			System.arraycopy(args, 1, newArgs, 0, args.length - 1);
			runInBackground = job.runInBackground();
		}

		JobManager mgr = null;
		try {
			mgr = JobManager.parseArgs(job, newArgs);
			if (mgr.hasHelp()) {
				JobManager.printHelp(job);
				return;
			}
			if (runInBackground) {
				forkProcess(job, mgr.getJobID(), newArgs);
				return;
			}

			mgr.saveArgs();
			mgr.lock();
			JobManager p = null;
			if (job.getJobGroup().getParentJobGroup() != null) {
				p = mgr.getParentJobManager();
				p.lock();
			}
			job.run(mgr);
			if (p != null) {
				p.unLock();
			}
			mgr.unLock();
		} catch (ParseException|MissingOptionException e) {
			JobManager.printHelp(job);
			System.err.println(e.getMessage());
		}
	}

	public static void main(String [] args) throws Exception {
		String confFile = "";
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("NLPCONF=")) {
				confFile = args[i].substring("NLPCONF=".length(), args[i].length());
				System.out.println("Use configuration: " + new File(confFile).getAbsolutePath());
			}
		}
		List<NLPJob> jobs = new ArrayList<NLPJob>();
		jobs.add(new GitLogCorpusJob());
		jobs.add(new HashFilesCorpusJob());
		jobs.add(new TextCorpusJob());
		jobs.add(new StableLinuxCorpusJob());
		jobs.add(new LDAJob());
		jobs.add(new LDADumpJob());
		jobs.add(new ExtractGroupJob());
		jobs.add(new TopDownJob());
		jobs.add(new TopDownDumpJob());
		jobs.add(new BottomUpJob());
		jobs.add(new BottomUpGraphJob());
		jobs.add(new GAUnderClassJob());
		jobs.add(new PatchClusterJob());
		jobs.add(new TopicTrendJob());
		jobs.add(new LoadBugResultJob());
		jobs.add(new CompareWithManualJob());

		(new CliMain(jobs, confFile)).run(args);
	}
}
