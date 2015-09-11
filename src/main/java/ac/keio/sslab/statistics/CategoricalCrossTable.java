package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;
import weka.core.converters.ArffSaver;
import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.SimpleSorter;

public class CategoricalCrossTable {

	static public class RawRecord {
		private Set<String> subjectiveVar;
		private String objectiveVar;

		public RawRecord(Set<String> subjectiveVariableNames, String objectiveVariableName) {
			this.subjectiveVar = new HashSet<String>();
			this.subjectiveVar.addAll(subjectiveVariableNames);
			this.objectiveVar = objectiveVariableName;
		}

		public Set<String> getSubjectiveVariables() {
			Set<String> ret = new HashSet<String>();
			ret.addAll(subjectiveVar);
			return ret;
		}
		
		public boolean hasSubjectiveVariableName(String subjectiveVariableName) {
			return subjectiveVar.contains(subjectiveVariableName);
		}

		public String getObjectiveVariable() {
			return objectiveVar;
		}
	}

	static public class Record {
		private Map<String, Integer> subjectiveVar;
		private String objectiveVar;
		
		public Record(Map<String, Integer> subjectiveVariable, String objectiveVariable) {
			this.subjectiveVar = new HashMap<String, Integer>();
			this.subjectiveVar.putAll(subjectiveVariable);
			this.objectiveVar = objectiveVariable;
		}

		public Map<String, Integer> getSubjectiveVariables() {
			Map<String, Integer> ret = new HashMap<String, Integer>();
			ret.putAll(subjectiveVar);
			return ret;
		}
		
		public int subjectiveVariableNameCount(String subjectiveVariableName) {
			return subjectiveVar.get(subjectiveVariableName);
		}

		public String getObjectiveVariable() {
			return objectiveVar;
		}
	}

	Map<String, Record> allRecords;
	Set<String> objectiveVariables;
	private CategoricalCrossTable() {
		objectiveVariables = new HashSet<String>();
		allRecords = new HashMap<String, Record>();
	}

	public static CategoricalCrossTable loadFromRawData(Map<String, RawRecord> rawRecords) {
		//first, get all of subjective and objective names
		CategoricalCrossTable table = new CategoricalCrossTable();
		Set<String> subjectiveNames = new HashSet<String>();
		for (Entry<String, RawRecord> e: rawRecords.entrySet()) {
			subjectiveNames.addAll(e.getValue().getSubjectiveVariables());
			table.objectiveVariables.add(e.getValue().getObjectiveVariable());
		}

		//second, fill up all the subjective and objective variable columns by 0 or 1 for each row
		for (Entry<String, RawRecord> e: rawRecords.entrySet()) {
			HashMap<String, Integer> subjectiveOne = new HashMap<String, Integer>();
			
			RawRecord one = e.getValue();
			for (String name: subjectiveNames) {
				subjectiveOne.put(name, one.hasSubjectiveVariableName(name) ? 1 : 0);
			}
			table.allRecords.put(e.getKey(), new Record(subjectiveOne, one.getObjectiveVariable()));
		}
		return table;
	}

	public Set<String> getSubjectiveVariableNames() {
		if (allRecords.size() == 0) {
			return new HashSet<String>();
		}
		Record one = allRecords.values().iterator().next();
		return one.getSubjectiveVariables().keySet();
	}

	public Set<String> getObjectiveVariableNames() {
		Set<String> ret = new HashSet<String>();
		ret.addAll(objectiveVariables);
		return ret;
	}

	public int countSubjectiveVariable(String variableName) {
		int count = 0;
		for (Record r: allRecords.values()) {
			count += r.subjectiveVariableNameCount(variableName);
		}

		return count;
	}

	public Map<String, Integer> countAllSubjectVariable() {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		for (String name: getSubjectiveVariableNames()) {
			counts.put(name, 0);
		}

		for (Record r: allRecords.values()) {
			for (String name: r.getSubjectiveVariables().keySet()) {
				counts.put(name, counts.get(name) + r.subjectiveVariableNameCount(name));
			}
		}
		return counts;
	}

	public Map<String, Integer> countSubjectVariablesForObjectiveVariable(String objectiveVarName) {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		for (String name: getSubjectiveVariableNames()) {
			counts.put(name, 0);
		}

		for (Record r: allRecords.values()) {
			if (!r.getObjectiveVariable().equals(objectiveVarName)) {
				continue;
			}
			for (String name: r.getSubjectiveVariables().keySet()) {
				counts.put(name, counts.get(name) + r.subjectiveVariableNameCount(name));
			}
		}
		return counts;
	}

	public int countObjectiveVariable(String variableName) {
		int count = 0;
		for (Record r: allRecords.values()) {
			if (r.getObjectiveVariable().equals(variableName)) {
				count += 1;
			}
		}

		return count;
	}

	public Map<String, List<Integer>> getBinaryOfSubjectiveVar() {
		Map<String, List<Integer>> ret = new HashMap<String, List<Integer>>();
		Set<String> subjectNames = new TreeSet<String>();
		subjectNames.addAll(getSubjectiveVariableNames());

		for (Entry<String, Record> e: allRecords.entrySet()) {
			List<Integer> binaryList = new ArrayList<Integer>();
			Record one = e.getValue();
			for (String subjectName: subjectNames) {
				binaryList.add(one.subjectiveVariableNameCount(subjectName));
			}
			ret.put(e.getKey(), binaryList);
		}
		return ret;
	}

	public void writeVarTable(File outFile) {
		PrintWriter writer = null;
		try {
			outFile.getParentFile().mkdirs();
			writer = JobUtils.getPrintWriter(outFile);

			StringBuilder sb = new StringBuilder();
			sb.append("keyID, objectiveVar");

			Set<String> subjectNames = new TreeSet<String>();
			subjectNames.addAll(getSubjectiveVariableNames());
			for (String subjectName: subjectNames) {
				sb.append(',').append('"').append(subjectName).append('"');
			}
			writer.println(sb.toString());

			Map<String, List<Integer>> binaryTable = getBinaryOfSubjectiveVar();
			for (Entry<String, List<Integer>> e: binaryTable.entrySet()) {
				sb.setLength(0);
				sb.append('"').append(e.getKey()).append('"');
				sb.append(',').append('"').append(allRecords.get(e.getKey()).getObjectiveVariable()).append('"');
				for (int i: e.getValue()) {
					sb.append(',').append(i);
				}
				writer.println(sb.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (writer != null) {
			writer.close();
		}
	}

	public void writeCSV(File outFile) {
		Map<String, List<Integer>> count = new HashMap<String, List<Integer>>();
		Map<String, Integer> subjectiveCount = countAllSubjectVariable();
		List<Entry<String, Integer>> subjectiveNames = SimpleSorter.reverse(subjectiveCount);
		for (String objectiveVarName: objectiveVariables) {
			Map<String, Integer> countOne = countSubjectVariablesForObjectiveVariable(objectiveVarName);
			List<Integer> countList = new ArrayList<Integer>();
			for (Entry<String, Integer> subjective: subjectiveNames) {
				countList.add(countOne.get(subjective.getKey()));
			}
			count.put(objectiveVarName, countList);
		}
		List<String> objectiveNames = new ArrayList<String>();
		objectiveNames.addAll(objectiveVariables);
		Collections.sort(objectiveNames);
		PrintWriter writer = null;
		try {
			writer = JobUtils.getPrintWriter(outFile);
			StringBuilder sb = new StringBuilder();
			for (Entry<String, Integer> subjectiveName: subjectiveNames) {
				sb.append(',').append(subjectiveName.getKey());
			}
			writer.println(sb.toString());
			for (String objectiveName: objectiveNames) {
				sb.setLength(0);
				sb.append(objectiveName);
				for (int sbjCount: count.get(objectiveName)) {
					sb.append(',').append(sbjCount);
				}
				writer.println(sb.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (writer != null) {
			writer.close();
		}
	}

	public static CategoricalCrossTable readCSV(File inFile) {
		CategoricalCrossTable table = new CategoricalCrossTable();
		try {
			BufferedReader br = new BufferedReader(new FileReader(inFile));
			//first, get all of names
			String line = br.readLine();
			if (line == null) {
				System.err.println("WARNING: read empty file: " + inFile.getAbsolutePath());
				br.close();
				return null;
			}

			List<String> names = new ArrayList<String>();
			for (String str: line.split(",")) {
				names.add(str);
			}

			line = br.readLine();
			while (line != null) {
				//second, fill up all the subjective and objective variable columns for each row
				String [] strs = line.split(",");
				table.objectiveVariables.add(strs[1]);
				HashMap<String, Integer> subjectiveOne = new HashMap<String, Integer>();
				for (int i = 2; i < strs.length; i++) {
					subjectiveOne.put(names.get(i), Integer.parseInt(strs[i]));
				}
				table.allRecords.put(strs[0], new Record(subjectiveOne, strs[1]));
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return table;
	}

	public Instances getArff() {
		FastVector attributes = new FastVector();
		attributes.addElement(new Attribute("keyID", (FastVector) null)); //null means string attribute in ARFF format...

		FastVector objectiveNominal = new FastVector();
		for (String objectiveVarName: getObjectiveVariableNames()) {
			objectiveNominal.addElement(objectiveVarName);
		}

		Set<String> subjectNames = new TreeSet<String>();
		subjectNames.addAll(getSubjectiveVariableNames());
		for (String subjectName: subjectNames) {
			attributes.addElement(new Attribute(subjectName)); //append numeric attributes
		}
		attributes.addElement(new Attribute("objectiveVar", objectiveNominal)); //append a nominal attribute

		Instances ret = new Instances("CrossTable", attributes, 0);

		Map<String, List<Integer>> binaryTable = getBinaryOfSubjectiveVar();
		for (Entry<String, List<Integer>> e: binaryTable.entrySet()) {
			List<Integer> vals = new ArrayList<Integer>();
			vals.add(ret.attribute(0).addStringValue(e.getKey()));
			for (int i: e.getValue()) {
				vals.add(i);
			}
			vals.add(objectiveNominal.indexOf(allRecords.get(e.getKey()).getObjectiveVariable()));
			double [] array = new double[vals.size()];
			int i = 0;
			for (int val: vals) {
				array[i++] = (double)val;
			}
			ret.add(new Instance(1.0, array));
		}

		ret.setClassIndex(ret.numAttributes() - 1);
		
		return ret;
	}

	public static CategoricalCrossTable loadArffData(Instances arff) {
		CategoricalCrossTable ret = new CategoricalCrossTable();
		//first, get all of names
		List<String> names = new ArrayList<String>();
		for (int i = 0; i < arff.numAttributes(); i++) {
			names.add(arff.attribute(i).name());
		}
		for (int i = 0; i < arff.numInstances(); i++) {
			Instance record = arff.instance(i);
			String ID = record.attribute(0).toString();
			HashMap<String, Integer> subjectiveOne = new HashMap<String, Integer>();
			for (int j = 1; j < record.numAttributes() - 1; j++) {
				subjectiveOne.put(names.get(j), (int)Double.parseDouble(record.attribute(j).toString()));
			}
			String objectiveVarName = record.attribute(record.numAttributes() - 1).toString();
			ret.allRecords.put(ID, new Record(subjectiveOne, objectiveVarName));
		}
		return ret;
	}

	public void writeArff(File outFile) {
		try {
			outFile.getParentFile().mkdirs();
			ArffSaver saver = new ArffSaver();
			saver.setInstances(getArff());
			saver.setFile(outFile);
			saver.writeBatch();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static CategoricalCrossTable readArff(File inFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inFile));
			ArffReader arff = new ArffReader(reader);
			Instances data = arff.getData();
			data.setClassIndex(data.numAttributes() - 1);
			return loadArffData(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
