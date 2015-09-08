package ac.keio.sslab.statistics;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.JobManager;
import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.nlp.NLPConf;
import ac.keio.sslab.nlp.NLPJobGroup;
import ac.keio.sslab.nlp.SingletonGroupNLPJob;

public class LoadBugResultJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "loadBugResult";
	}

	@Override
	public String getShortJobName() {
		return "br";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return null;
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, "statistics");
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "Load bug analysis results in json format";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("d", "directory", true, "json directory path for results"));
		OptionGroup g2 = new OptionGroup();
		g2.addOption(new Option("e", "error", true, "json file path for error class"));
		g.setRequired(true);
		g2.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		opt.addOptionGroup(g2);
		return opt;
	}

	private void writeErrorClass(Map<String, String> errorToClass, Map<String, String> errorToSubClass, File outDir) {
		Map<String, Set<String>> classToSubClass = new HashMap<String, Set<String>>();
		for (Entry<String, String> e: errorToClass.entrySet()) {
			if (!classToSubClass.containsKey(e.getValue())) {
				classToSubClass.put(e.getValue(), new HashSet<String>());
			}
			classToSubClass.get(e.getValue()).add(errorToSubClass.get(e.getKey()));
		}

		Map<String, Set<String>> subClassToError = new HashMap<String, Set<String>>();
		for (Entry<String, String> e: errorToSubClass.entrySet()) {
			if (!subClassToError.containsKey(e.getValue())) {
				subClassToError.put(e.getValue(), new HashSet<String>());
			}
			subClassToError.get(e.getValue()).add(e.getKey());
		}
		
		try {
			PrintWriter writer = JobUtils.getPrintWriter(new File(outDir, "errorclass.txt"));
			StringBuilder sb = new StringBuilder();
			for (Entry<String, Set<String>> e: classToSubClass.entrySet()) {
				writer.println("errorclass:" + e.getKey());
				for (String subClass: e.getValue()) {
					sb.setLength(0);
					sb.append(subClass).append(':');
					for (String error: subClassToError.get(subClass)) {
						sb.append(' ').append(error).append(',');
					}
					sb.setLength(sb.length() - 1);
					writer.println(sb.toString());
				}
				writer.println();
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to write error class tables");
		}
	}

	private void writeResults(BugResult result, File varDir, File csvDir, File arffDir, File countDir, String name) {
		CategoricalCrossTable table = result.toCategoricalCrossTable();
		table.writeVarTable(new File(varDir, name + ".csv"));
		table.writeCSV(new File(csvDir, name + ".csv"));
		table.writeArff(new File(arffDir, name + ".arff"));

		Map<String, Integer> count = table.countAllSubjectVariable();
		List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
		list.addAll(count.entrySet());
		Collections.sort(list ,new Comparator<Entry<String, Integer>>(){
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		try {
			PrintWriter writer = JobUtils.getPrintWriter(new File(countDir, name + ".csv"));
			int total = 0;
			for (Entry<String, Integer> e: list) {
				writer.println(e.getKey() + "," + Integer.toString(e.getValue()));
				total += e.getValue();
			}
			writer.println("total," + Integer.toString(total));
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to write count result: " + countDir.getAbsolutePath());
		}
	}

	@Override
	public void run(JobManager mgr) {
		File outDir = new File("statistics", mgr.getArgStr("j"));
		File csvDir = new File(outDir, "csv");
		File varDir = new File(outDir, "obj-by-sbj-var");
		File arffDir = new File(outDir, "arff");
		File countDir = new File(outDir, "count");
		if (csvDir.exists() || arffDir.exists()) {
			if (!JobUtils.promptDeleteDirectory(csvDir, mgr.doForceWrite()) || !JobUtils.promptDeleteDirectory(arffDir, mgr.doForceWrite())) {
				return;
			}
		}
		csvDir.getParentFile().mkdirs();
		arffDir.getParentFile().mkdirs();

		try {
			BugResult result = BugResult.loadFromDirectory(new File(mgr.getArgStr("d")), new File(mgr.getArgStr("e")));
			writeResults(result, varDir, csvDir, arffDir, countDir, "bulk");
			writeResults(result.getFixTypeByCluster(), varDir, csvDir,arffDir, countDir, "fixtype");

			BugResult bugs = result.extractBugs();
			Map<String, String> errorToClass = bugs.getErrorToClassMap();
			Map<String, String> errorToSubClass = bugs.getErrorToSubClassMap();
			writeErrorClass(errorToClass, errorToSubClass, outDir);

			writeResults(bugs.getFixedTopDirectoryByCluster(), varDir, csvDir, arffDir, countDir, "fixedtopdir");
			writeResults(bugs.getFixedSubDirectoryByCluster("drivers"), varDir, csvDir, arffDir, countDir, "driversdir");
			writeResults(bugs.getFixedSubSubDirectoryByCluster("drivers", "net"), varDir, csvDir, arffDir, countDir, "driversnetdir");
			writeResults(bugs.getFixedSubSubDirectoryByCluster("drivers", "media"), varDir, csvDir, arffDir, countDir, "driversmediadir");
			writeResults(bugs.getFixedSubDirectoryByCluster("arch"), varDir, csvDir, arffDir, countDir, "archdir");
			writeResults(bugs.getErrorByCluster(), varDir, csvDir, arffDir, countDir, "error");
			writeResults(bugs.getErrorClassByCluster(), varDir, csvDir, arffDir, countDir, "errorclass");
			writeResults(bugs.getErrorClassByFailureSite(), varDir, csvDir, arffDir, countDir, "errorclass-by-failuresite");
			writeResults(bugs.getErrorClassByTrigger(), varDir, csvDir, arffDir, countDir, "errorclass-by-trigger");
			writeResults(bugs.getErrorClassByTopDirectory(), varDir, csvDir, arffDir, countDir, "errorclass-by-topdir");
			writeResults(bugs.getErrorClassBySubDirectory("drivers"), varDir, csvDir, arffDir, countDir, "errorclass-by-driversdir");
			writeResults(bugs.getErrorClassBySubSubDirectory("drivers", "net"), varDir, csvDir, arffDir, countDir, "errorclass-by-driversnetdir");
			writeResults(bugs.getErrorClassBySubSubDirectory("drivers", "media"), varDir, csvDir, arffDir, countDir, "errorclass-by-driversmediadir");
			writeResults(bugs.getErrorClassBySubDirectory("arch"), varDir, csvDir, arffDir, countDir, "errorclass-by-archdir");

			File suberrorVar = new File(varDir, "errorsubclass");
			File suberrorCSV = new File(csvDir, "errorsubclass");
			File suberrorArff = new File(arffDir, "errorsubclass");
			File suberrorCount = new File(countDir, "errorsubclass");
			suberrorCSV.mkdirs();
			suberrorArff.mkdirs();
			suberrorCount.mkdirs();
			for (String className: errorToClass.values()) {
				String fileName = className.replace(' ', '-').replace(':', '_');
				writeResults(bugs.getErrorSubClassByCluster(className), suberrorVar, suberrorCSV, suberrorArff, suberrorCount, fileName);
				writeResults(bugs.getErrorSubClassByTrigger(className), suberrorVar, suberrorCSV, suberrorArff,suberrorCount,  fileName + "-by-trigger");
				writeResults(bugs.getErrorSubClassByFailureSite(className), suberrorVar, suberrorCSV, suberrorArff,suberrorCount,  fileName + "-by-failuresite");
				if (className.equals("hard state") || className.equals("soft state")) {
					writeResults(bugs.getComponentByClusterWithErrorClassName(className), suberrorVar, suberrorCSV, suberrorArff, suberrorCount,
							fileName + "-with-loc");
				}
			}

			writeResults(bugs.getCrashByCluster(), varDir, csvDir, arffDir, countDir, "crash");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
