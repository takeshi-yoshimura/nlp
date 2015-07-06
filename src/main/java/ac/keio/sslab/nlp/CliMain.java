package ac.keio.sslab.nlp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import ac.keio.sslab.statistics.LoadBugResultJob;

public class CliMain {
	protected Map<String, NLPJob> jobs;
	protected NLPConf conf;

	public CliMain() {
		jobs = new HashMap<String, NLPJob>();
		conf = new NLPConf();
		this.jobs.put("help", null);
		this.jobs.put("bg", null);
	}

	public CliMain(List<NLPJob> jobs) {
		this.jobs = new HashMap<String, NLPJob>();
		for (NLPJob job: jobs) {
			addJob(job);
		}
		this.jobs.put("help", null);
		this.jobs.put("fg", null);
		this.jobs.put("list", null);
	}

	public void addJob(NLPJob job) {
		jobs.put(job.getJobName(), job);
	}

	private void showJobs() {
		System.out.println("Available commands:");
		for (Entry<String, NLPJob> e: jobs.entrySet()) {
			if (e.getValue() != null) {
				System.out.println(e.getValue().getJobName() + "\t" + e.getValue().getJobDescription());
			}
		}
		System.out.println("list\tShow past job IDs");
		System.out.println("help\tShow available commands and their descriptions");
	}

	private Map<String, String> parseArgs(Options options, String [] args) throws ParseException {
		Map<String, String> argMap = new HashMap<String, String>();
		CommandLine line = (new PosixParser()).parse(options, args);
		for (Option opt: line.getOptions()) {
			argMap.put(opt.getOpt(), opt.getValue());
		}
		return argMap;
	}

	private Options getJobOptions(NLPJob job) {
		Options options = job.getOptions();
		if (options == null) {
			options = new Options();
		}
		if (options.hasOption("j") || options.hasOption("ow") || options.hasOption("force") || options.hasOption("h")) {
			System.err.println("Options -j --jobID, -ow --overwrite, -force --forceOverwrite, -h --help are used at " 
								+ this.getClass().getName());
			System.exit(-1);
			return null;
		}
		OptionGroup required = new OptionGroup();
		required.setRequired(true);
		required.addOption(new Option("j", "jobID", true, "ID of the job"));
		options.addOptionGroup(required);
		options.addOption("ow", "overwrite", false, "Overwrite (ask overwriting again later if no -f or --forceOverwrite)");
		options.addOption("force", "forceOverwrite", false, "Foce to overwrite (ignored if no --overwrite or -o)");
		options.addOption("h", "help", false, "Help");
		return options;
	}

	private void printHelp(String jobName, Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(jobName, options);
	}

	public void forkProcess(NLPJob job, String [] args) {
		Options options = getJobOptions(job);
		Map<String, String> argMap;
		try {
			argMap = parseArgs(options, args);
		} catch (Exception e) {
			System.err.println(e.toString());
			printHelp(job.getJobName(), options);
			return;
		}
		String jobID = argMap.get("j");
		ProcessBuilder pb = new ProcessBuilder();
		String dirPath = System.getProperty("user.dir");
		List<String> cmd = new ArrayList<String>();
		cmd.add("nohup");
		cmd.add(dirPath + File.separator + "nlp");
		cmd.add("fg"); //a hidden job name
		cmd.add(job.getJobName());
		for (String arg: args) {
			cmd.add(arg);
		}
		for (String c: cmd) {
			System.out.print(c + " ");
		}
		pb.command(cmd);
		NLPConf conf = new NLPConf();
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
				JobUtils.listJobs(args[1]);
			}
			return;
		} else {
			job = jobs.get(args[0]);
			newArgs = new String[args.length - 1];
			System.arraycopy(args, 1, newArgs, 0, args.length - 1);
			runInBackground = job.runInBackground();
		}

		if (job.getJobName() == NLPConf.snapshotJobName) {
			String [] newArgs2 = new String[newArgs.length + 2];
			System.arraycopy(newArgs, 0, newArgs2, 0, newArgs.length);
			newArgs2[newArgs.length - 2] = "-j";
			newArgs2[newArgs.length - 1] = "takeSnapshot";
			newArgs = newArgs2;
		}

		if (runInBackground) {
			forkProcess(job, newArgs);
			return;
		}

		JobManager manager = new JobManager(job.getJobName());
		Options options = getJobOptions(job);
		try {
			Map<String, String> argMap = parseArgs(options, newArgs);
			String jobID = argMap.get("j");
			if (!manager.tryLock(jobID)) {
				System.out.println("Currently the job " + job.getJobName() + " ID = " + jobID + " is running. Aborts");
				return;
			}
			if (argMap.containsKey("h")) {
				printHelp(job.getJobName(), options);
			} else if (manager.hasJobIDArgs(jobID) && !argMap.containsKey("ow")) {
				System.out.println("Found the past output for job " + job.getJobName() + " ID = " + jobID + ". Use the past arguments");
				job.run(manager.getJobIDArgs(jobID));
			} else {
				if (job.getJobName() != NLPConf.snapshotJobName) {
					manager.saveJobIDArgs(jobID, argMap);
				}
				job.run(argMap);
			}
			manager.unLock(jobID);
		} catch (ParseException e) {
			System.err.println(e.toString());
			printHelp(job.getJobName(), options);
		}
	}

	public static void main(String [] args) {
		List<NLPJob> jobs = new ArrayList<NLPJob>();
		jobs.add(new GitSetupJob());
		jobs.add(new GitCorpusJob());
		jobs.add(new DeduplicateCorpusJob());
		jobs.add(new LDAJob());
		jobs.add(new LDADumpJob());
		jobs.add(new ExtractGroupJob());
		jobs.add(new TopDownJob());
		jobs.add(new TopicTrendJob());
		jobs.add(new LinuxPatchJob());
		jobs.add(new LoadBugResultJob());

		jobs.add(new SnapshotManager(jobs));

		(new CliMain(jobs)).run(args);
	}
}
