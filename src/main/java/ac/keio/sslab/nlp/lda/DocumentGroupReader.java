package ac.keio.sslab.nlp.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

public class DocumentGroupReader {
	Map<Integer, String> docIndex;
	List<Pair<Double, Integer>> distance;
	Map<Integer, List<Integer>> docTopicId;
	Vector centroid;

	class FirstReverseSorter implements Comparator<Pair<Double, ?>> {
		@Override
		public int compare(Pair<Double, ?> p1, Pair<Double, ?> p2) {
			double d1 = p1.getFirst();
			double d2 = p2.getFirst();
			if (d1 > d2) {
				return -1;
			} else if (d1 == d2) {
				return 0;
			}
			return 1;
		}
	}

	class FirstSorter implements Comparator<Pair<Double, ?>> {
		@Override
		public int compare(Pair<Double, ?> p1, Pair<Double, ?> p2) {
			double d1 = p1.getFirst();
			double d2 = p2.getFirst();
			if (d1 < d2) {
				return -1;
			} else if (d1 == d2) {
				return 0;
			}
			return 1;
		}
	}

	public DocumentGroupReader(Path docIndex, Path documentDir, Set<Integer> group, Configuration conf, int maxTopics) throws IOException {
		loadDocumentIndex(docIndex, group, conf);
		loadDocumentDir(documentDir, group, conf, maxTopics);
	}

	public List<Pair<String, List<Integer>>> getDocuments() {
		List<Pair<String, List<Integer>>> ret = new ArrayList<Pair<String, List<Integer>>>();
		for (Pair<Double, Integer> doc: distance) {
			ret.add(new Pair<String, List<Integer>>(docIndex.get(doc.getSecond()), docTopicId.get(doc.getSecond())));
		}
		return ret;
	}
	
	public List<Integer> getCentroid(int maxTopics) {
		FirstReverseSorter sorter = new FirstReverseSorter();
		List<Pair<Double, Integer>> topic = new ArrayList<Pair<Double, Integer>>();
		for (Element e: centroid.all()) {
			topic.add(new Pair<Double, Integer>(e.get(), e.index()));
		}
		Collections.sort(topic, sorter);
		List<Integer> topicId = new ArrayList<Integer>();
		for (int i = 0; i < maxTopics && i < topic.size(); i++) {
			topicId.add(topic.get(i).getSecond());
		}
		return topicId;
	}

	public void loadDocumentIndex(Path docIndexPath, Set<Integer> group, Configuration conf) throws IOException {
		docIndex = new HashMap<Integer, String>();
		SequenceDirectoryReader<Integer, String> dictionaryReader = new SequenceDirectoryReader<>(docIndexPath, conf, Integer.class, String.class);
		while (dictionaryReader.seekNext()) {
			int documentId = dictionaryReader.key();
			if (group.contains(documentId)) {
				String documentName = dictionaryReader.val().toString();
				docIndex.put(documentId, documentName);
			}
		}
		dictionaryReader.close();
	}

	public void loadDocumentDir(Path documentDir,  Set<Integer> group, Configuration conf, int maxTopics) throws IOException {
		docTopicId = new HashMap<Integer, List<Integer>>();
		distance = new ArrayList<Pair<Double, Integer>>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(documentDir, conf, Integer.class, Vector.class);
		FirstReverseSorter sorter = new FirstReverseSorter();
		int numVec = 0;
		Map<Integer, Vector> vectors = new HashMap<Integer, Vector>();
		while (reader.seekNext()) {
			int docId = reader.key();
			if (!group.contains(docId)) {
				continue;
			}
			numVec++;
			Vector vector = reader.val();
			if (centroid == null) {
				centroid = new DenseVector(vector);
			} else {
				centroid = centroid.plus(vector);
			}
			vectors.put(docId, vector);
		}
		reader.close();
		centroid = centroid.divide(numVec);

		SquaredEuclideanDistanceMeasure measure = new SquaredEuclideanDistanceMeasure();
		for (Entry<Integer, Vector> e: vectors.entrySet()) {
			int docId = e.getKey();
			Vector vector = e.getValue();
			List<Pair<Double, Integer>> docTopic = new ArrayList<Pair<Double, Integer>>();
			for (Element e2: vector.all()) {
				docTopic.add(new Pair<Double, Integer>(e2.get(), e2.index()));
			}
			Collections.sort(docTopic, sorter);
			List<Integer> topicId = new ArrayList<Integer>();
			for (int i = 0; i < maxTopics && i < docTopic.size(); i++) {
				topicId.add(docTopic.get(i).getSecond());
			}
			docTopicId.put(docId, topicId);
			distance.add(new Pair<Double, Integer>(measure.distance(centroid, vector), docId));
		}
		Collections.sort(distance, new FirstSorter());
	}

}
