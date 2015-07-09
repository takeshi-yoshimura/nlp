package ac.keio.sslab.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector.Element;

public class MatrixColInRawSorter {
	protected Matrix valueSorted, colOrder;
	protected Map<Integer, Integer> rawOrder;
	protected Map<Integer, Double> rawEntropies;

	protected Comparator<Entry<Integer, Double>> sorter = new Comparator<Entry<Integer, Double>>() {
		public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
			return e1.getValue().compareTo(e2.getValue());
		}
	};
	protected Comparator<Entry<Integer, Double>> reverser = new Comparator<Entry<Integer, Double>>() {
		public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
			return e2.getValue().compareTo(e1.getValue());
		}
	};

	public MatrixColInRawSorter(Matrix toBeSorted) {
		valueSorted = new DenseMatrix(toBeSorted.rowSize(), toBeSorted.columnSize());
		colOrder = new DenseMatrix(toBeSorted.rowSize(), toBeSorted.columnSize());
		rawOrder = new HashMap<Integer, Integer>();
		rawEntropies = new HashMap<Integer, Double>();

		sortColInRaw(toBeSorted);
	}

	protected void sortColInRaw(Matrix toBeSorted) {
		Map<Integer, List<Entry<Integer, Double>>> colSortedRaws = new HashMap<Integer, List<Entry<Integer, Double>>>();

		for (MatrixSlice raw: toBeSorted) {
			Map<Integer, Double> cols = new HashMap<Integer, Double>();
			double entropy = 0.0;
			for (Element e: raw.getVector().all()) {
				cols.put(e.index(), e.get());
				entropy -= e.get() * Math.log(e.get()) / Math.log(2);
			}
			rawEntropies.put(raw.index(), entropy);
			List<Entry<Integer, Double>> sortedCol = new ArrayList<Entry<Integer, Double>>(cols.entrySet());
			Collections.sort(sortedCol, reverser);
			colSortedRaws.put(raw.index(), sortedCol);
		}

		List<Entry<Integer, Double>> sortedRawEntropies = new ArrayList<Entry<Integer, Double>>(rawEntropies.entrySet());
		Collections.sort(sortedRawEntropies, sorter);
		int rawIndex = 0;
		for (Entry<Integer, Double> rawEntropy: sortedRawEntropies) {
			int raw = rawEntropy.getKey();
			int colIndex = 0;
			for (Entry<Integer, Double> col: colSortedRaws.get(raw)) {
				valueSorted.set(rawIndex, colIndex, col.getValue());
				colOrder.set(rawIndex, colIndex++, col.getKey());
			}
			rawOrder.put(raw, rawIndex);
		}
	}

	public Matrix getValueMatrix() {
		return valueSorted;
	}

	public Matrix getColOrderMatrix() {
		return colOrder;
	}

	public Map<Integer, Integer> getRawOrderMap() {
		return rawOrder;
	}

	public Map<Integer, Double> getRawEntropyMap() {
		return rawEntropies;
	}
}
