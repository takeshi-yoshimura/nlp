package ac.keio.sslab.statistics;

import java.util.Set;
import java.util.TreeMap;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;

public class DocumentClassMatrix {
	protected Matrix docClass = null;
	protected TreeMap<Integer, String> docIndex;
	protected TreeMap<Integer, String> classIndex;
	
	protected DocumentClassMatrix(Matrix docClass, TreeMap<Integer, String> docIndex, TreeMap<Integer, String> classIndex) {
		this.docClass = docClass;
		this.docIndex = docIndex;
		this.classIndex = classIndex;
	}
	
	public Matrix times(DocumentClassMatrix right) {
		return docClass.times(right.docClass);
	}
	
	public DocumentClassMatrix transpose() {
		return new DocumentClassMatrix(docClass.transpose(), docIndex, classIndex);
	}
	
	public TreeMap<Integer, String> getDocIndex() {
		return docIndex;
	}
	
	public TreeMap<Integer, String> getClassIndex() {
		return classIndex;
	}
	
	public DocumentClassMatrix dropRows(Set<Integer> droppedRows) {
		Matrix newDocClass = new DenseMatrix(docClass.rowSize(), docClass.columnSize());
		TreeMap<Integer, String> newDocIndex = new TreeMap<Integer, String>();
		int index = 0;
		for (MatrixSlice slice: docClass) {
			if (droppedRows.contains(slice.index()))
				continue;
			newDocIndex.put(index, docIndex.get(slice.index()));
			newDocClass.assignColumn(index++, slice.getVector());
		}
		return new DocumentClassMatrix(newDocClass, newDocIndex, classIndex);
	}
}
