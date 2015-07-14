package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.VectorWritable;
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

	// build matrix from csv (line: {strA,strB,strC,...}) -> raw: strA, col: strB, strC,...
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

	public NamedMatrix normalizeRow() {
		Matrix newMatrix = new SparseMatrix(matrix.rowSize(), matrix.columnSize());
		for (MatrixSlice slice: matrix) {
			newMatrix.assignRow(slice.index(), slice.getVector().normalize());
		}
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(newMatrix, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	static public NamedMatrix buildFromLDAFiles(LDAHDFSFiles hdfs, Configuration hdfsConf, String rawGroupName, String colGroupName) {
		TreeMap<Integer, String> colIndex = new TreeMap<Integer, String>();
		try {
			for (Entry<Integer, List<String>> e: new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, hdfsConf, 2).getTopics().entrySet()) {
				colIndex.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
		} catch (Exception e) {
			System.err.println("Failed to load HDFS files " + hdfs.dictionaryPath + " or " + hdfs.topicPath + ": " + e.getMessage());
			return null;
		}

		TreeMap<Integer, String> rawIndex = new TreeMap<Integer, String>(colIndex);
		try {
			SequenceDirectoryReader dictionaryReader = new SequenceDirectoryReader(hdfs.docIndexPath, hdfsConf);
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (dictionaryReader.next(key, value)) {
				int documentId = key.get();
				String documentName = value.toString();
				rawIndex.put(documentId, documentName);
			}
			dictionaryReader.close();
		} catch (Exception e) {
			System.err.println("Failed to load HDFS file " + hdfs.docIndexPath + ": " + e.getMessage());
			return null;
		}

		Matrix matrix = new DenseMatrix(rawIndex.size(), colIndex.size());
		try {
			SequenceDirectoryReader reader = new SequenceDirectoryReader(hdfs.documentPath, hdfsConf);
			IntWritable key = new IntWritable();
			VectorWritable value = new VectorWritable();
			while (reader.next(key, value)) {
				for (Element p: value.get().nonZeroes()) {
					matrix.set(key.get(), p.index(), p.get());
				}
			}
			reader.close();
		} catch (Exception e) {
			System.err.println("Failed to loead HDFS file " + hdfs.docIndexPath + " or " + hdfs.documentPath + ": " + e.getMessage());
			return null;
		}

		return new NamedMatrix(matrix, rawIndex, colIndex, rawGroupName, colGroupName);
	}

	public NamedMatrix times(NamedMatrix right) {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(right.colIndex);
		return new NamedMatrix(matrix.times(right.matrix), newRowIndex, newColIndex, rowGroupName, right.colGroupName);
	}

	public NamedMatrix transpose() {
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(colIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(rowIndex);
		return new NamedMatrix(matrix.transpose(), newRowIndex, newColIndex, colGroupName, rowGroupName);
	}

	public int rowSize() {
		return matrix.rowSize();
	}

	public int colSize() {
		return matrix.columnSize();
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
		Matrix newMatrix = new DenseMatrix(matrix.rowSize() - droppedRows.size(), matrix.columnSize());
		TreeMap<Integer, String> newRawIndex = new TreeMap<Integer, String>();
		int index = 0;
		for (MatrixSlice slice: matrix) {
			if (droppedRows.contains(slice.index()))
				continue;
			newRawIndex.put(index, rowIndex.get(slice.index()));
			newMatrix.assignRow(index++, slice.getVector());
		}
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);
		return new NamedMatrix(newMatrix, newRawIndex, newColIndex, rowGroupName, colGroupName);
	}

	public void dumpCSV(File out) throws Exception {
		PrintWriter pw = JobUtils.getPrintWriter(out);
		StringBuilder sb = new StringBuilder("#" + rowGroupName);
		for (Entry<Integer, String> col: colIndex.entrySet()) {
			sb.append(',').append(col.getValue());
		}
		pw.println(sb.toString());

		for (MatrixSlice slice: matrix) {
			sb.setLength(0);
			sb.append(colIndex.get(slice.index()));
			for (Element p: slice.all()) {
				sb.append(',').append(p.get());
			}
			pw.println(sb.toString());
		}
		pw.close();
	}

	public ColNamedMatrix buildColSorted() {
		return ColNamedMatrix.build(matrix, rowIndex, colIndex, rowGroupName, colGroupName);
	}
}
