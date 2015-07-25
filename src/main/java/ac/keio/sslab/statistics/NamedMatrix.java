package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class NamedMatrix {
	protected Matrix matrix = null;
	protected TreeMap<Integer, String> rowIndex, colIndex;
	protected String rowGroupName, colGroupName;

	protected NamedMatrix(Matrix matrix, TreeMap<Integer, String> rowIndex, TreeMap<Integer, String> colIndex, String rowGroupName, String colGroupName) {
		this.matrix = matrix;
		this.rowIndex = rowIndex;
		this.colIndex = colIndex;
		this.rowGroupName = rowGroupName;
		this.colGroupName = colGroupName;
	}

	static public NamedMatrix buildOne(int rowSize, int colSize) {
		TreeMap<Integer, String> rowIndex = new TreeMap<Integer, String>();
		TreeMap<Integer, String> colIndex = new TreeMap<Integer, String>();
		Matrix one = new DenseMatrix(rowSize, colSize);
		for (int i = 0; i < rowSize; i++) {
			rowIndex.put(i, "one");
			for (int j = 0; j < colSize; j++) {
				one.set(i, j, 1.0);
				colIndex.put(j, "one");
			}
		}

		return new NamedMatrix(one, rowIndex, colIndex, "one", "one");
	}

	// build matrix from csv (line: {strA,strB,strC,...}) -> row: strA, col: strB, strC,...
	static public NamedMatrix buildFromCSV(File inputFile, String rowGroupName, String colGroupName) {
		Map<Integer, List<Integer>> rowCols = new HashMap<Integer, List<Integer>>();
		TreeMap<Integer, String> rowIndex = new TreeMap<Integer, String>();
		TreeMap<Integer, String> colIndex = new TreeMap<Integer, String>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			Map<String, Integer> revTagNames = new HashMap<String, Integer>();

			// split a single record into {docIndex, doc}, {tagIndex, tag}
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				String[] splitLine = line.split(",");
				List<Integer> colIndices = new ArrayList<Integer>();
				for (int i = 1; i < splitLine.length; i++) {
					if (!revTagNames.containsKey(splitLine[i])) {
						revTagNames.put(splitLine[i], colIndex.size());
						colIndex.put(colIndex.size(), splitLine[i]);
					}
					colIndices.add(revTagNames.get(splitLine[i]));
				}
				rowCols.put(rowIndex.size(), colIndices);
				rowIndex.put(rowIndex.size(), splitLine[0]);
			}
			br.close();
		} catch (Exception e) {
			System.err.println("Failed to load Local file " + inputFile.getAbsolutePath() + ": " + e.getMessage());
			return null;
		}

		Matrix matrix = new SparseMatrix(rowCols.size(), colIndex.size());
		for (Entry<Integer, List<Integer>> e: rowCols.entrySet()) {
			for (int index: e.getValue()) {
				matrix.set(e.getKey(), index, 1.0);
			}
		}

		return new NamedMatrix(matrix, rowIndex, colIndex, rowGroupName, colGroupName);
	}

	// TODO: this is naive
	public NamedMatrix normalizeRow() {
		Matrix newMatrix = new SparseMatrix(matrix.rowSize(), matrix.columnSize());
		for (MatrixSlice slice: matrix) {
			newMatrix.assignRow(slice.index(), slice.getVector().normalize(1));
		}
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(newMatrix, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	static public NamedMatrix buildFromLDAFiles(LDAHDFSFiles hdfs, Configuration hdfsConf, String rowGroupName, String colGroupName) {
		TreeMap<Integer, String> colIndex = new TreeMap<Integer, String>();
		try {
			for (Entry<Integer, List<String>> e: new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, hdfsConf, 2).getTopics().entrySet()) {
				colIndex.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
		} catch (Exception e) {
			System.err.println("Failed to load HDFS files " + hdfs.dictionaryPath + " or " + hdfs.topicPath + ": " + e.getMessage());
			return null;
		}

		TreeMap<Integer, String> rowIndex = new TreeMap<Integer, String>(colIndex);
		try {
			SequenceDirectoryReader<Integer, String> dictionaryReader = new SequenceDirectoryReader<>(hdfs.docIndexPath, hdfsConf);
			while (dictionaryReader.seekNext()) {
				rowIndex.put(dictionaryReader.key(), dictionaryReader.val());
			}
			dictionaryReader.close();
		} catch (Exception e) {
			System.err.println("Failed to load HDFS file " + hdfs.docIndexPath + ": " + e.getMessage());
			return null;
		}

		Matrix matrix = new DenseMatrix(rowIndex.size(), colIndex.size());
		try {
			SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(hdfs.documentPath, hdfsConf);
			while (reader.seekNext()) {
				for (Element p: reader.val().nonZeroes()) {
					matrix.set(reader.key(), p.index(), p.get());
				}
			}
			reader.close();
		} catch (Exception e) {
			System.err.println("Failed to loead HDFS file " + hdfs.docIndexPath + " or " + hdfs.documentPath + ": " + e.getMessage());
			return null;
		}

		return new NamedMatrix(matrix, rowIndex, colIndex, rowGroupName, colGroupName);
	}

	public NamedMatrix times(NamedMatrix right) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(right.colIndex);
		return new NamedMatrix(matrix.times(right.matrix), newRowIndex, newColIndex, rowGroupName, right.colGroupName);
	}

	public NamedMatrix times(double v) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(matrix.times(v), newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public NamedMatrix transpose() {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(colIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(rowIndex);
		return new NamedMatrix(matrix.transpose(), newRowIndex, newColIndex, colGroupName, rowGroupName);
	}

	public NamedMatrix divide(double v) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(matrix.divide(v), newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public NamedMatrix timesOneByOne(NamedMatrix right) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		Matrix newMatrix = new SparseMatrix(matrix.rowSize(), matrix.columnSize());
		for (int i = 0; i < rowIndex.size(); i++) {
			for (int j = 0; j < colIndex.size(); j++) {
				newMatrix.set(i, j, matrix.get(i, j) * right.matrix.get(i, j));
			}
		}
		return new NamedMatrix(newMatrix, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public NamedMatrix divideOneByOne(NamedMatrix right) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		Matrix newMatrix = new SparseMatrix(matrix.rowSize(), matrix.columnSize());
		for (int i = 0; i < rowIndex.size(); i++) {
			for (int j = 0; j < colIndex.size(); j++) {
				newMatrix.set(i, j, matrix.get(i, j) / right.matrix.get(i, j));
			}
		}
		return new NamedMatrix(newMatrix, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public NamedMatrix plus(NamedMatrix right) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(matrix.plus(right.matrix), newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public int rowSize() {
		return matrix.rowSize();
	}

	public int colSize() {
		return matrix.columnSize();
	}

	public void setRowGroupName(String rowGroupName) {
		this.rowGroupName = rowGroupName;
	}

	public void setColGroupName(String colGroupName) {
		this.colGroupName = colGroupName;
	}

	public TreeMap<Integer, String> lostRowIndex(NamedMatrix other) {
		Set<String> lostRows = new HashSet<String>(rowIndex.values());
		lostRows.removeAll(other.rowIndex.values());

		TreeMap<Integer, String> lostRowIndex = new TreeMap<Integer, String>();
		for (Entry<Integer, String> e: rowIndex.entrySet()) {
			if (lostRows.contains(e.getValue())) {
				lostRowIndex.put(e.getKey(), e.getValue());
			}
		}
		return lostRowIndex;
	}

	public NamedMatrix dropRows(Set<Integer> droppedRows) {
		Matrix newMatrix = new SparseMatrix(matrix.rowSize() - droppedRows.size(), matrix.columnSize());
		TreeMap<Integer, String> newrowIndex = new TreeMap<Integer, String>();
		int index = 0;
		for (MatrixSlice slice: matrix) {
			if (droppedRows.contains(slice.index()))
				continue;
			newrowIndex.put(index, rowIndex.get(slice.index()));
			newMatrix.assignRow(index++, slice.getVector());
		}
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(newMatrix, newrowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public void dumpCSVInMatrixFormat(File out) throws Exception {
		PrintWriter pw = JobUtils.getPrintWriter(out);
		StringBuilder sb = new StringBuilder();
		sb.append('#').append(rowGroupName);
		for (Entry<Integer, String> col: colIndex.entrySet()) {
			sb.append(',').append(col.getValue());
		}
		pw.println(sb.toString());

		for (MatrixSlice slice: matrix) {
			sb.setLength(0);
			sb.append(rowIndex.get(slice.index()));
			for (Element p: slice.all()) {
				sb.append(',').append(p.get());
			}
			pw.println(sb.toString());
		}
		pw.close();
	}

	public void dumpCSVInKeyValueFormat(File out) throws Exception {
		Comparator<Entry<String, Double>> reverser = new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> e1, Entry<String, Double> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		};

		Map<String, Double> values = new HashMap<String, Double>();
		for (MatrixSlice slice: matrix) {
			for (Element p: slice.all()) {
				values.put(rowIndex.get(slice.index()) + "," + colIndex.get(p.index()), p.get());
			}
		}
		List<Entry<String, Double>> sortedValues = new ArrayList<Entry<String, Double>>(values.entrySet());
		Collections.sort(sortedValues, reverser);

		PrintWriter pw = JobUtils.getPrintWriter(out);
		StringBuilder sb = new StringBuilder();
		sb.append('#').append(rowGroupName).append(colGroupName);
		pw.println(sb.toString());

		for (Entry<String, Double> value: sortedValues) {
			sb.setLength(0);
			sb.append(value.getKey()).append(',').append(value.getValue());
			pw.println(sb.toString());
		}
		pw.close();
	}

	public ColNamedMatrix buildColSorted() {
		return ColNamedMatrix.build(matrix, rowIndex, colIndex, rowGroupName, colGroupName);
	}
}
