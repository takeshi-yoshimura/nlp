package ac.keio.sslab.nlp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.utils.SimpleJsonReader;
import ac.keio.sslab.utils.SimpleJsonWriter;

public class JobManager {

	NLPJobGroup job;
	Map<String, String> args;

	protected JobManager(NLPJobGroup job, Map<String, String> args) {
		this.job = job;
		this.args = args;
	}

	static public JobManager parseArgs(NLPJob job, String [] args) throws ParseException,MissingOptionException {
		Map<String, String> a = new HashMap<>();

		Options options = getFinalOptions(job);
		if (options == null) {
			throw new ParseException("Duplicated options -j --jobID, -ow --overwrite, or -h --help for job " + job.getAlgorithmName());
		}
		CommandLine line = (new PosixParser()).parse(options, args);
		for (Option opt: line.getOptions()) {
			a.put(opt.getOpt(), opt.getValue());
		}

		return new JobManager(job.getJobGroup(), a);
	}

	public static Options getFinalOptions(NLPJob job) {
		Options options = job.getOptions();
		if (options == null) {
			options = new Options();
		}
		if (options.hasOption("j") || options.hasOption("ow") || options.hasOption("h")) {
			return null;
		}

		OptionGroup required = new OptionGroup();
		required.setRequired(true);
		required.addOption(new Option("j", "jobID", true, "ID of the job"));
		options.addOptionGroup(required);
		OptionGroup parent = new OptionGroup();
		parent.setRequired(true);
		NLPJobGroup p = job.getJobGroup().getParentJobGroup();
		parent.addOption(new Option(p.getShortJobName(), p.getJobName(), true, "ID for " + p.getJobDescription() + " job"));
		options.addOptionGroup(parent);
		options.addOption("ow", "overwrite", false, "Force to overwrite");
		options.addOption("h", "help", false, "Help");

		return options;
	}

	static public JobManager restoreArgs(NLPJob job, String jobID) throws Exception {
		return new JobManager(job.getJobGroup(), readJobArgs(job.getAlgorithmName()).get(jobID));
	}

	interface StrParser {
		Object parse(String str);
	}
	static final Map<Class<?>, StrParser> ps = new HashMap<>();
	static {
		ps.put(Integer.class, new StrParser() { public Object parse(String str) { return Integer.parseInt(str); }});
		ps.put(String.class, new StrParser() { public Object parse(String str) { return str; }});
		ps.put(Double.class, new StrParser() { public Object parse(String str) { return Double.parseDouble(str); }});
		ps.put(Long.class, new StrParser() { public Object parse(String str) { return Long.parseLong(str); }});
		ps.put(Boolean.class, new StrParser() { public Object parse(String str) { return Boolean.parseBoolean(str); }});
	};

	@SuppressWarnings("unchecked")
	public <T> T getArgOrDefault(String key, T defaultValue, Class<T> c) {
		if (args.containsKey(key)) {
			return (T) ps.get(c).parse(args.get(key));
		}
		return defaultValue;
	}

	public String getArgStr(String key) {
		return args.get(key);
	}

	public String getJobID() {
		return args.get("j");
	}

	public String getParentJobID() {
		return args.get(job.getShortJobName());
	}

	public File getLocalOutputDir() {
		return new File(job.getLocalJobDir(), getJobID());
	}

	public Path getHDFSOutputDir() {
		return new Path(job.getHDFSJobDir(), getJobID());
	}

	public boolean hasHelp() {
		return args.containsKey("h");
	}

	static public void printHelp(NLPJob job) {
		Options options = getFinalOptions(job);
		if (options == null) {
			System.err.println("Duplicated options -j --jobID, -ow --overwrite, or -h --help for job " + job.getAlgorithmName());
			return;
		}
		new HelpFormatter().printHelp(job.getJobGroup().getJobName(), options);
	}

	public boolean doForceWrite() {
		return args.containsKey("ow");
	}

	public boolean hasArg(String key) {
		return args.containsKey(key);
	}

	public JobManager getParentJobManager() throws Exception {
		return new JobManager(job.getParentJobGroup(), readJobArgs(job.getParentJobGroup().getJobName()).get(getParentJobID()));
	}

	public void saveArgs() throws IOException {
		Map<String, Map<String, String>> jobArgs;
		File argFile = new File(NLPConf.getInstance().localArgFile, job.getJobName());
		if (!argFile.exists()) {
			NLPConf.getInstance().localArgFile.mkdirs();
			argFile.createNewFile();
			jobArgs = new HashMap<>();
		} else {
			jobArgs = readJobArgs(job.getJobName());
		}
		if (jobArgs.containsKey(getJobID())) {
			jobArgs.remove(getJobID());
		}
		writeJobArgs(argFile, jobArgs);
	}

	static protected Map<String, Map<String, String>> readJobArgs(String jobTypeName) throws IOException {
		File f = new File(NLPConf.getInstance().localArgFile, jobTypeName);
		Map<String, Map<String, String>> ret = new HashMap<>();
		SimpleJsonReader reader = new SimpleJsonReader(f);
		while (!reader.isCurrentTokenEndObject()) {
			String jobID = reader.getCurrentFieldName();
			reader.readStartObject(jobID);
			Map<String, String> jobArg = new HashMap<>();
			while (!reader.isCurrentTokenEndObject()) {
				String key = reader.getCurrentFieldName();
				String value = reader.readStringField(key);
				jobArg.put(key, value);
			}
			ret.put(jobID, jobArg);
		}
		reader.close();
		return ret;
	}

	protected void writeJobArgs(File f, Map<String, Map<String, String>> arg) throws IOException {
		SimpleJsonWriter writer = new SimpleJsonWriter(f);
		for (Entry<String, Map<String, String>> e: arg.entrySet()) {
			writer.writeStartObject(e.getKey());
			for (Entry<String, String> e2: e.getValue().entrySet()) {
				writer.writeStringField(e2.getKey(), e2.getValue());
			}
			writer.writeEndObject();
		}
		writer.close();
	}

	public void lock() throws IOException {
		File lock = new File(NLPConf.getInstance().localLockFile, job.getJobName() + "/" + getJobID());
		if (lock.exists()) {
			throw new IOException(getJobID() + " job (" + job.getJobName() + ") output files are in use");
		}
		lock.getParentFile().mkdirs();
		lock.createNewFile();
		lock.deleteOnExit(); //Does this work?
	}

	public void unLock() throws IOException {
		File lock = new File(NLPConf.getInstance().localLockFile, job.getJobName() + "/" + getJobID());
		if (lock.exists()) {
			//lock.delete();
		} else {
			System.err.println("WARNING: Inconsistent unlock");
		}
	}
}
