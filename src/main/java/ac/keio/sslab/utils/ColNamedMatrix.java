package ac.keio.sslab.utils;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.nlp.job.JobUtils;

public class ColNamedMatrix {
	protected Matrix value, col;
	protected TreeMap<Integer, String> rowIndex, colIndex;
	protected String rowGroupName, colGroupName;

	protected ColNamedMatrix(Matrix value, Matrix col, TreeMap<Integer, String> rowIndex, TreeMap<Integer, String> colIndex, String rowGroupName, String colGroupName) {
		this.value = value;
		this.col = col;
		this.rowIndex = rowIndex;
		this.colIndex = colIndex;
		this.rowGroupName = rowGroupName;
		this.colGroupName = colGroupName;
	}

	static public ColNamedMatrix build(Matrix matrix, TreeMap<Integer, String> rowIndex, TreeMap<Integer, String> colIndex, String rowGroupName, String colGroupName) {
		Matrix value = matrix.clone();
		Matrix col = new DenseMatrix(matrix.rowSize(), matrix.columnSize());
		for (int i = 0; i < matrix.rowSize(); i++) {
			for (int j = 0; j < matrix.columnSize(); j++) {
				col.set(i, j, j);
			}
		}
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);

		return new ColNamedMatrix(value, col, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public ColNamedMatrix colSortedByValue() {
		Matrix newValue = new DenseMatrix(value.rowSize(), value.columnSize());
		Matrix newCol = new DenseMatrix(col.rowSize(), col.columnSize());
		for (MatrixSlice row: value) {
			Map<Integer, Double> colValues = new HashMap<Integer, Double>();
			for (Element e: row.getVector().all()) {
				colValues.put((int)col.get(row.index(), e.index()), e.get());
			}
			List<Entry<Integer, Double>> sortedColValues = SimpleSorter.reverse(colValues);
			for (int i = 0; i < sortedColValues.size(); i++) {
				newCol.set(row.index(), i, sortedColValues.get(i).getKey());
				newValue.set(row.index(), i, sortedColValues.get(i).getValue());
			}
		}
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>(rowIndex);
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);

		return new ColNamedMatrix(newValue, newCol, newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public ColNamedMatrix rowSortedByEntropy() {
		Comparator<Entry<Integer, Double>> sorter = new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
				return e1.getValue().compareTo(e2.getValue());
			}
		};

		List<Entry<Integer, Double>> sortedRowEntropies = new ArrayList<Entry<Integer, Double>>(calculateRowEntropy().entrySet());
		Collections.sort(sortedRowEntropies, sorter);

		Matrix newValue = new DenseMatrix(value.rowSize(), value.columnSize());
		TreeMap<Integer, String> newRowIndex = new TreeMap<Integer, String>();
		for (int i = 0; i < sortedRowEntropies.size(); i++) {
			int origRow = sortedRowEntropies.get(i).getKey();
			newRowIndex.put(i, rowIndex.get(origRow));
			for (int j = 0; j < value.columnSize(); j++) {
				newValue.set(i, j, value.get(origRow, j));
			}
		}
		TreeMap<Integer, String> newColIndex = new TreeMap<Integer, String>(colIndex);

		return new ColNamedMatrix(newValue, col.clone(), newRowIndex, newColIndex, rowGroupName, colGroupName);
	}

	public Map<Integer, Double> calculateRowEntropy() {
		Map<Integer, Double> result = new HashMap<Integer, Double>();
		for (MatrixSlice row: value) {
			double entropy = 0.0;
			for (Element e: row.getVector().all()) {
				entropy -= e.get() * Math.log(e.get()) / Math.log(2);
			}
			result.put(row.index(), entropy);
		}

		return result;
	}

	public void dumpCSV(File out, boolean displayEntropy) throws Exception {
		PrintWriter pw = JobUtils.getPrintWriter(out);
		StringBuilder sb = new StringBuilder();
		Map<Integer, Double> entropies = null;
		if (displayEntropy) {
			entropies = calculateRowEntropy();
		}

		for (MatrixSlice valueRow: value) {
			String rowName = rowIndex.get(valueRow.index());
			sb.append(rowGroupName);
			if (displayEntropy) {
				sb.append(",Entropy");
			}
			for (int i = 0; i < col.columnSize(); i++) {
				sb.append(',').append(colIndex.get((int)col.get(valueRow.index(), i)));
			}
			pw.println(sb.toString());
			sb.setLength(0);

			sb.append(rowName);
			if (displayEntropy) {
				sb.append(',').append(entropies.get(valueRow.index()));
			}

			for (Element e: valueRow.all()) {
				sb.append(',').append(e.get());
			}
			pw.println(sb.toString());
			sb.setLength(0);
		}
		pw.close();
	}
}
