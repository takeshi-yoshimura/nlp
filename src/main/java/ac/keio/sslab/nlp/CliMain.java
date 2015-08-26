package ac.keio.sslab.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import ac.keio.sslab.statistics.CompareWithManualJob;
import ac.keio.sslab.statistics.LoadBugResultJob;

public class CliMain {
	protected Map<String, NLPJob> jobs;
	protected NLPConf conf;

	public CliMain(String confFile) {
		jobs = new HashMap<String, NLPJob>();
		this.conf = NLPConf.getInstance();
		this.conf.loadConfFile(confFile);
		this.jobs.put("help", null);
		this.jobs.put("bg", null);
	}

	public CliMain(List<NLPJob> jobs, String confFile) {
		this.jobs = new HashMap<String, NLPJob>();
		this.conf = NLPConf.getInstance();
		this.conf.loadConfFile(confFile);
		for (NLPJob job: jobs) {
			this.jobs.put(job.getJobName(), job);
		}
		this.jobs.put("help", null);
		this.jobs.put("fg", null);
		this.jobs.put("list", null);
	}

	public void showJobs() {
		System.out.println("Available commands:");
		for (Entry<String, NLPJob> e: jobs.entrySet()) {
			if (e.getValue() != null) {
				System.out.println(e.getValue().getJobName() + "\t" + e.getValue().getJobDescription());
			}
		}
		System.out.println("list\tShow past job IDs");
		System.out.println("help\tShow available commands and their descriptions");
	}

	public void listJobs(String jobName) {
		NLPConf conf = NLPConf.getInstance();
		File argFile = new File(conf.localArgFile, jobName);
		argFile.getParentFile().mkdirs();
		try {
			if (!argFile.exists()) {
				System.out.println("Job " + jobName + " has never been invoked");
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
		} catch (Exception e) {
			System.err.println("List up job IDs failed: " + e.toString());
		}
	}

	public void forkProcess(NLPJob job, String [] args) {
		JobManager manager = new JobManager(job);
		try {
			manager.parseOptions(args);
		} catch (Exception e) {
			manager.printHelp();
			System.err.println(e.getMessage());
			return;
		}
		String jobID = manager.getJobID();
		ProcessBuilder pb = new ProcessBuilder();
		List<String> cmd = new ArrayList<String>();
		cmd.add("nohup");
		cmd.add(conf.runPath);
		cmd.add("fg"); //a hidden job name
		cmd.add(job.getJobName());
		for (String arg: args) {
			cmd.add(arg);
		}
		for (String c: cmd) {
			System.out.print(c + " ");
		}
		pb.command(cmd);
		File outputDir = new File(conf.localLogFile, job.getJobName() + "/" + jobID);
		outputDir.mkdirs();
		File stdoutFile = new File(outputDir, "stdout");
		File stderrFile = new File(outputDir, "stderr");
		pb.redirectOutput(stdoutFile);
		pb.redirectError(stderrFile);
		pb.redirectInput(new File("/dev/null"));

		System.out.println("The process for a job remains running in a background");
		System.out.println("Stdout: " + stdoutFile.getAbsolutePath());
		System.out.println("Stderr: " + stderrFile.getAbsolutePath());
		try {
			pb.start(); //restart this class from calling main()
		} catch (IOException e) {
			System.err.println("A job running attempt failed: " + e.toString());
		}
	}

	public void run(String [] args) {
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
			if (args.length == 1 || !jobs.containsKey(args[1])) {
				System.err.println("Need to specify job type Name");
			} else {
				listJobs(args[1]);
			}
			return;
		} else {
			job = jobs.get(args[0]);
			newArgs = new String[args.length - 1];
			System.arraycopy(args, 1, newArgs, 0, args.length - 1);
			runInBackground = job.runInBackground();
		}

		JobManager manager = new JobManager(job);
		try {
			manager.parseBasicArgs(newArgs);
			if (manager.hasHelp()) {
				manager.printHelp();
				return;
			}
			String jobID = manager.getJobID();
			if (manager.hasPastArgs()) {
				System.out.println("Found the past output for job " + job.getJobName() + " ID = " + jobID + ". Use the past arguments");
				String [] pastArgs = manager.readPastArgs();
				if (manager.doForceWrite()) {
					newArgs = new String[pastArgs.length + 1];
					System.arraycopy(pastArgs, 0, newArgs, 0, pastArgs.length);
					newArgs[newArgs.length - 1] = "-ow";
				} else {
					newArgs = pastArgs;
				}
			}
			if (runInBackground) {
				forkProcess(job, newArgs);
				return;
			}
			manager.parseOptions(newArgs);

			if (!manager.tryLock()) {
				System.out.println("Currently the job " + job.getJobName() + " ID = " + jobID + " is running. Aborts");
				return;
			}
			manager.saveArgs();
			job.run(manager);
			manager.unLock();
		} catch (Exception e) {
			e.printStackTrace();
			manager.printHelp();
		}
	}

	public static void main(String [] args) {
		String confFile = "";
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("NLPCONF=")) {
				confFile = args[i].substring("NLPCONF=".length(), args[i].length());
				System.out.println("Use configuration: " + new File(confFile).getAbsolutePath());
			}
		}
		List<NLPJob> jobs = new ArrayList<NLPJob>();
		jobs.add(new GitCorpusJob());
		jobs.add(new TextCorpusJob());
		jobs.add(new DeduplicateCorpusJob());
		jobs.add(new LDAJob());
		jobs.add(new LDADumpJob());
		jobs.add(new ExtractGroupJob());
		jobs.add(new TopDownJob());
		jobs.add(new TopDownDumpJob());
		jobs.add(new BottomUpJob());
		jobs.add(new BottomUpGraphJob());
		jobs.add(new ClusteringResultJob());
		jobs.add(new ClassificationJob());
		jobs.add(new PatchMetricsJob());
		jobs.add(new TopicTrendJob());
		jobs.add(new LoadBugResultJob());
		jobs.add(new CompareWithManualJob());

		(new CliMain(jobs, confFile)).run(args);
	}
}
