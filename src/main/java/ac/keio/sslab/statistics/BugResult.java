package ac.keio.sslab.statistics;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import ac.keio.sslab.utils.CategoricalCrossTable;
import ac.keio.sslab.utils.CategoricalCrossTable.RawRecord;

public class BugResult {

	private BugResult() {
		raw = new HashMap<String, RawRecord>();
		errorToClass = new HashMap<String, String>();
		errorToSubClass = new HashMap<String, String>();
	}

	private Map<String, RawRecord> raw;
	private Map<String, String> errorToClass;
	private Map<String, String> errorToSubClass;

	protected Set<String> getStringsInJSON(JSONObject fixJson, String keyName) {
		Object obj = fixJson.get(keyName);
		Set<String> ret = new HashSet<String>();
		if (obj instanceof JSONArray) {
			JSONArray triggerArray = (JSONArray) obj;
			for (int i = 0; i < triggerArray.length(); i++) {
				ret.add(triggerArray.getString(i));
			}
		} else {
			ret.add((String) obj);
		}
		return ret;
	}

	protected Set<String> loadOneFix(JSONObject fixJson) {
		Set<String> ret = new HashSet<String>();
		String failureSite = fixJson.getString("when").split(":")[0];
		if (failureSite.equals("build")) {
			ret.add("fixclass:build");
			return ret;
		} else {
			ret.add("fixclass:bug");
		}
		ret.add("failuresite:" + failureSite);

		for (String fixedLoc: getStringsInJSON(fixJson, "fix")) {
			String [] locs = fixedLoc.split(":")[0].split("/");
			ret.add("fixedloc:" + locs[0]); //Top directory in Linux kernel
			if (locs.length -1 > 1) { //skip .c
				ret.add("fixedsubloc-" + locs[0] + ":" + locs[1]);
				if (locs.length -1 > 2) {
					ret.add("fixedsubsubloc-" + locs[0] + "-" + locs[1] + ":" + locs[2]);
				} else {
					ret.add("fixedsubsubloc-" + locs[0] + "-" + locs[1] + ":.");
				}
			} else {
				ret.add("fixedsubloc-" + locs[0] + ":.");
			}
		}
		for (String trigger: getStringsInJSON(fixJson, "if")) {
			ret.add("trigger:" + (trigger.equals("") ? "None": trigger.split(":")[0]));
		}
		for (String error: getStringsInJSON(fixJson, "errors")) {
			String [] errors = error.split(":");
			ret.add("error:" + errors[0]);
			if (errors[0].equals("crash")){
				ret.add("crash:" + ((errors.length > 1) ? errors[1]: "None"));
			}

			if (!errorToClass.containsKey(errors[0])) {
				System.out.println("WARNING:unclassified error: " + errors[0]);
			} else {
				String errorClass = errorToClass.get(errors[0]);
				String errorSubClass = errorToSubClass.get(errors[0]);
				ret.add("errorclass:" + errorClass);
				ret.add("errorsubclass-" + errorClass + ":" + errorSubClass);
				ret.add("errorsubsubclass-" + errorClass + "-" + errorSubClass + ":" + errors[0]);
			}
		}

		return ret;
	}

	public static BugResult loadFromDirectory(File dirFile, File errorClassFile) throws Exception {
		BugResult ret = new BugResult();
		//create Error -> error class and subclass Map
		ret.loadErrorClassFromJson(errorClassFile);

		int shaCount = 0;
		for (File jsonFile: dirFile.listFiles()) {
			if (!jsonFile.getName().contains(".json")) {
				System.err.println("WARNING: BugResult.loadBugJson() only loads .json files. ignore " + jsonFile.getAbsolutePath());
				continue;
			}
			JSONObject bugJson = null;
			try {
				bugJson = new JSONObject(IOUtils.toString(new FileInputStream(jsonFile)));
			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Detected an error at " + jsonFile.getAbsolutePath());
			}

			String objectiveVarName = jsonFile.getName().substring(0, jsonFile.getName().length() - ".json".length());
			for (Object sha: bugJson.keySet()) {
				Object fixType = bugJson.getJSONObject((String)sha).get("type");
				try {
					if (fixType instanceof JSONObject) {
						JSONObject fixJson = (JSONObject) fixType;
						if (fixJson.keySet().size() == 0) {
							continue;
						}
						Set<String> subjectiveVarNames = ret.loadOneFix(fixJson);
						RawRecord oneRecord = new RawRecord(subjectiveVarNames, objectiveVarName);
						ret.raw.put((String)sha, oneRecord);
					} else if (fixType instanceof JSONArray) {
						JSONArray fixArray = (JSONArray) fixType;
						for (int i = 0; i < fixArray.length(); i++) {
							Set<String> subjectiveVarNames = ret.loadOneFix(fixArray.getJSONObject(i));
							RawRecord oneRecord = new RawRecord(subjectiveVarNames, objectiveVarName);
							ret.raw.put(sha + "-F" + Integer.toString(i), oneRecord);
						}
					} else {
						String nonBugName = (String) fixType;
						Set<String> subjectiveVarNames = new HashSet<String>();
						subjectiveVarNames.add("fixclass:" + nonBugName.split(":")[0]);
						RawRecord oneRecord = new RawRecord(subjectiveVarNames, objectiveVarName);
						ret.raw.put((String)sha, oneRecord);
					}
					shaCount++;
				} catch (Exception e) {
					e.printStackTrace();
					throw new Exception("Detected Errors at reading " + sha + " in " + jsonFile.getAbsolutePath());
				}
			}
		}
		System.out.println("Loaded " + shaCount + " shas");
		return ret;
	}

	public void loadErrorClassFromJson(File errorClassFile)	throws Exception{
		JSONObject errorJson = new JSONObject(IOUtils.toString(new FileInputStream(errorClassFile)));
		for (Object className: errorJson.keySet()) {
			JSONObject classJson = errorJson.getJSONObject((String)className);
			for (Object subClassName: classJson.keySet()) {
				JSONArray errors = classJson.getJSONArray((String)subClassName);
				for (int i = 0; i < errors.length(); i++) {
					errorToClass.put(errors.getString(i), (String)className);
					errorToSubClass.put(errors.getString(i), (String)subClassName);
				}
			}
		}
	}

	public CategoricalCrossTable toCategoricalCrossTable() {
		return CategoricalCrossTable.loadFromRawData(raw);
	}

	public BugResult getOnlySubjectiveVarNameStartsWith(String str) {
		BugResult ret = new BugResult();
		for (Entry<String, RawRecord> e: raw.entrySet()) {
			Set<String> newSubjective = new HashSet<String>();
			for (String subjectiveVarName: e.getValue().getSubjectiveVariables()) {
				if (subjectiveVarName.startsWith(str)) {
					newSubjective.add(subjectiveVarName.substring(str.length()));
				}
			}
			ret.raw.put(e.getKey(), new RawRecord(newSubjective, e.getValue().getObjectiveVariable()));
		}
		ret.errorToClass.putAll(errorToClass);
		ret.errorToSubClass.putAll(errorToSubClass);
		return ret;
	}

	public BugResult getFixTypeByCluster() {
		return getOnlySubjectiveVarNameStartsWith("fixclass:");
	}

	public BugResult getFixedTopDirectoryByCluster() {
		return getOnlySubjectiveVarNameStartsWith("fixedloc:");
	}

	public BugResult getFixedSubDirectoryByCluster(String name) {
		return getOnlySubjectiveVarNameStartsWith("fixedsubloc-" + name + ":");
	}

	public BugResult getFixedSubSubDirectoryByCluster(String name, String name2) {
		return getOnlySubjectiveVarNameStartsWith("fixedsubsubloc-" + name + "-" + name2 + ":");
	}

	public BugResult getErrorByCluster() {
		return getOnlySubjectiveVarNameStartsWith("error:");
	}

	public BugResult getErrorClassByCluster() {
		return getOnlySubjectiveVarNameStartsWith("errorclass:");
	}

	public BugResult getErrorSubClassByCluster(String className) {
		return getOnlySubjectiveVarNameStartsWith("errorsubclass-" + className + ":");
	}

	public BugResult getErrorSubSubClassByCluster(String className, String subClassName) {
		return getOnlySubjectiveVarNameStartsWith("errorsubsubclass-" + className + "-" + subClassName + ":");
	}

	public BugResult getCrashByCluster() {
		return getOnlySubjectiveVarNameStartsWith("crash:");
	}

	public BugResult getOnlySubjectiveVarNameMatchWith(String regex) {
		BugResult ret = new BugResult();
		for (Entry<String, RawRecord> e: raw.entrySet()) {
			Set<String> newSubjective = new HashSet<String>();
			for (String subjectiveVarName: e.getValue().getSubjectiveVariables()) {
				if (subjectiveVarName.matches(regex)) {
					newSubjective.add(subjectiveVarName);
				}
			}
			ret.raw.put(e.getKey(), new RawRecord(newSubjective, e.getValue().getObjectiveVariable()));
		}
		ret.errorToClass.putAll(errorToClass);
		ret.errorToSubClass.putAll(errorToSubClass);
		return ret;
	}

	public BugResult getNewSubjectiveVarAndObjectiveVarStartsWith(String subjectiveStartsWith, String objectiveStartsWith) {
		BugResult ret = new BugResult();
		for (Entry<String, RawRecord> e: raw.entrySet()) {
			Set<String> newSubjective = new HashSet<String>();
			Set<String> newObjective = new HashSet<String>();
			for (String subjectiveVarName: e.getValue().getSubjectiveVariables()) {
				if (subjectiveVarName.startsWith(subjectiveStartsWith)) {
					newSubjective.add(subjectiveVarName.substring(subjectiveStartsWith.length()));
				} else if (subjectiveVarName.startsWith(objectiveStartsWith)) {
					newObjective.add(subjectiveVarName.substring(objectiveStartsWith.length()));
				}
			}
			int i = 0;
			for (String objectiveVarName: newObjective) {
				ret.raw.put(e.getKey() + "-O" + Integer.toString(i++), new RawRecord(newSubjective, objectiveVarName));
			}
		}
		ret.errorToClass.putAll(errorToClass);
		ret.errorToSubClass.putAll(errorToSubClass);
		return ret;
	}

	public BugResult getErrorClassByTrigger() {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorclass:", "trigger:");
	}

	public BugResult getErrorSubClassByTrigger(String className) {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorsubclass-" + className + ":", "trigger:");
	}

	public BugResult getErrorClassByFailureSite() {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorclass:", "failuresite:");
	}

	public BugResult getErrorSubClassByFailureSite(String className) {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorsubclass-" + className + ":", "failuresite:");
	}

	public BugResult getComponentByClusterWithErrorClassName(String str) {
		BugResult ret = new BugResult();
		for (Entry<String, RawRecord> e: raw.entrySet()) {
			Set<String> newSubjective = new HashSet<String>();
			boolean hasStrError = false;
			for (String subjectiveVarName: e.getValue().getSubjectiveVariables()) {
				String errorClass = "errorclass:" + str;
				String locClass = "fixedsubloc-";
				if (subjectiveVarName.equals(errorClass)) {
					hasStrError = true;
				} else if (subjectiveVarName.startsWith(locClass)) {
					newSubjective.add(subjectiveVarName.substring(locClass.length()).replace(':', '/'));
				}
			}
			if (hasStrError) {
				ret.raw.put(e.getKey(), new RawRecord(newSubjective, e.getValue().getObjectiveVariable()));
			}
		}
		ret.errorToClass.putAll(errorToClass);
		ret.errorToSubClass.putAll(errorToSubClass);
		return ret;
	}

	public Map<String, String> getErrorToClassMap() {
		Map<String, String> ret = new HashMap<String, String>();
		ret.putAll(errorToClass);
		return ret;
	}

	public Map<String, String> getErrorToSubClassMap() {
		Map<String, String> ret = new HashMap<String, String>();
		ret.putAll(errorToSubClass);
		return ret;
	}

	public BugResult extractRawsOnlySubjectiveVarBelongs(String name) {
		BugResult ret = new BugResult();
		for (Entry<String, RawRecord> e: raw.entrySet()) {
			if (e.getValue().getSubjectiveVariables().contains(name)) {
				ret.raw.put(e.getKey(), e.getValue());
			}
		}
		ret.errorToClass.putAll(errorToClass);
		ret.errorToSubClass.putAll(errorToSubClass);
		return ret;
	}

	public BugResult extractBugs() {
		return extractRawsOnlySubjectiveVarBelongs("fixclass:bug");
	}

	public BugResult getErrorClassByTopDirectory() {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorclass:", "fixedloc:");
	}

	public BugResult getErrorClassBySubDirectory(String name) {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorclass:", "fixedsubloc-" + name + ":");
	}

	public BugResult getErrorClassBySubSubDirectory(String name, String name2) {
		return getNewSubjectiveVarAndObjectiveVarStartsWith("errorclass:", "fixedsubsubloc-" + name + "-" + name2 + ":");
	}
}
