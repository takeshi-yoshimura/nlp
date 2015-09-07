package ac.keio.sslab.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

public class JobManager {

	protected NLPConf conf = NLPConf.getInstance();
	protected File argFile;
	protected File lockFile;

	Options options;
	String jobID;
	Map<String, String> args;
	NLPJob job;

	public JobManager(NLPJob job) {
		this.job = job;
		options = job.getOptions();
		if (options == null) {
			options = new Options();
		}
		if (options.hasOption("j") || options.hasOption("ow") || options.hasOption("h")) {
			System.err.println("Ignore options -j --jobID, -ow --overwrite, -h --help at "  + this.getClass().getName());
		}
		OptionGroup required = new OptionGroup();
		required.setRequired(true);
		required.addOption(new Option("j", "jobID", true, "ID of the job"));
		options.addOptionGroup(required);
		options.addOption("ow", "overwrite", false, "Force to overwrite");
		options.addOption("h", "help", false, "Help");

		args = new HashMap<String, String>();

		argFile = new File(conf.localArgFile, job.getJobName());
		lockFile = new File(conf.localLockFile, job.getJobName());
		argFile.getParentFile().mkdirs();
		lockFile.mkdirs();
	}

	@SuppressWarnings("rawtypes")
	public void parseBasicArgs(String [] arguments) throws ParseException {
		OptionGroup required = new OptionGroup();
		required.setRequired(true);
		required.addOption(new Option("j", "jobID", true, "ID of the job"));
		Options jobOpts = new Options();
		jobOpts.addOption("h", "help", true, "Help");
		jobOpts.addOptionGroup(required);

		Collection opts = options.getOptions();
		for (Iterator i = opts.iterator(); i.hasNext();) {
			jobOpts.addOption((Option)i.next());
		}

		CommandLine line = (new PosixParser()).parse(jobOpts, arguments, false);
		for (Option opt: line.getOptions()) {
			args.put(opt.getOpt(), opt.getValue());
		}
		this.jobID = args.get("j");
	}

	public void parseOptions(String [] arguments) throws ParseException {
		CommandLine line = (new PosixParser()).parse(options, arguments);
		for (Option opt: line.getOptions()) {
			args.put(opt.getOpt(), opt.getValue());
		}
		this.jobID = args.get("j");
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

	public Path getArgJobIDPath(Path p, String key) {
		return new Path(p, args.get(key));
	}

	public Path getJobIDPath(Path p) {
		return getArgJobIDPath(p, "j");
	}

	public File getLocalArgFile(File f, String key) {
		return new File(f, args.get(key));
	}

	public String getJobID() {
		return args.get("j");
	}

	public boolean hasHelp() {
		return args.containsKey("h");
	}

	public void printHelp() {
		new HelpFormatter().printHelp(job.getJobName(), options);
	}

	public boolean doForceWrite() {
		return args.containsKey("ow");
	}

	public boolean hasArg(String key) {
		return args.containsKey(key);
	}

	public void addArg(String key, String value) {
		args.put(key, value);
	}

	public Options getOptions() {
		return options;
	}

	public boolean tryLock() throws IOException {
		File lock = new File(lockFile, jobID);
		if (lock.exists()) {
			return false;
		} else {
			lock.createNewFile();
			lock.deleteOnExit(); //Does this work?
			return true;
		}
	}

	public void unLock() throws IOException {
		File lock = new File(lockFile, jobID);
		if (lock.exists()) {
			//lock.delete();
		} else {
			System.err.println("WARNING: Inconsistent unlock");
		}
	}

	public void saveArgs() throws IOException {
		FileInputStream inputStream;
		JSONObject jobJson;
		if (!argFile.exists()) {
			jobJson = new JSONObject();
			argFile.createNewFile();
			inputStream = new FileInputStream(argFile);
		} else {
			inputStream = new FileInputStream(argFile);
			jobJson = new JSONObject(IOUtils.toString(inputStream));
		}
		if (jobJson.has(jobID)) {
			jobJson.remove(jobID);
		}
		jobJson.put(jobID, args);
        inputStream.close();
        FileOutputStream outputStream = new FileOutputStream(argFile);
        outputStream.write(jobJson.toString(4).getBytes());
        outputStream.close();
	}

	public NLPConf getNLPConf() {
		return conf;
	}
}
