package ac.keio.sslab.test.eclipse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.clustering.bottomup.CachedBottomupClustering;
import ac.keio.sslab.clustering.bottomup.IndexBottomupClustering;

public class BottomupClusteringTest {

	public static void main(String [] args) throws Exception {
		/* put and run testing function here */
	}

	public static void testBottomupClusteringCache(long memoryCapacity) throws Exception {
		List<Vector> vecs = new ArrayList<Vector>();
		int card = 300;
		int nump = 30000;

		for (int i = 0; i < nump; i++) {
			Vector vec = new DenseVector(card);
			for (int j = 0; j < card; j++) {
				double d = Math.random();
				vec.set(j, d);
			}
			vec = vec.normalize(1);
			vecs.add(vec);
		}

		CachedBottomupClustering clustering = new CachedBottomupClustering(vecs, memoryCapacity);
		System.out.println("cache initialization finished");
		int i = 0;
		while (true) {
			int [] nextPair1 = clustering.popMostSimilarClusterPair();
			if (nextPair1 == null) {
				break;
			}
			System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1]);
			++i;
		}
	}

	public static void testBottomupClusteringMany(long memoryCapacity) throws Exception {
		for (int i = 0; i < 100; i++) {
			System.out.println("iteration #" + i);
			testBottomupClustering(false, memoryCapacity);
		}
	}

	public static void testBottomupClustering(boolean showIterations, long memoryCapacity) throws Exception {
		List<Vector> vecs = new ArrayList<Vector>();
		int card = 2;
		int nump = 300;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < nump; i++) {
			Vector vec = new DenseVector(card);
			for (int j = 0; j < card; j++) {
				double d = Math.random();
				vec.set(j, d);
			}
			vec = vec.normalize(1);
			vecs.add(vec);

			sb.append("vecs.add(new DenseVector(new double[] {");
			for (Element e: vec.all()) {
				sb.append(e.get()).append(',');
			}
			sb.setLength(sb.length() - 1);
			sb.append("}));\n");
		}

		CachedBottomupClustering clustering = new CachedBottomupClustering(vecs, memoryCapacity);
		NaiveBottomupClustering basic = new NaiveBottomupClustering(vecs);
		int i = 0;
		List<Integer> badAt = new ArrayList<Integer>();
		while (true) {
			int [] nextPair1 = clustering.popMostSimilarClusterPair();
			int [] nextPair2 = basic.next();
			if (nextPair1 == null && nextPair2 == null) {
				break;
			}
			boolean ok = (nextPair1[0] == nextPair2[0]) && (nextPair1[1] == nextPair2[1]);
			if (showIterations) {
				System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1] + " =" + clustering.getMaxSimilarity() + ", " 
									+ nextPair2[0] + ", " + nextPair2[1] + " = " + basic + " (" + ok + ")");
			}
			if (!ok) {
				badAt.add(i);
			}
			++i;
		}
		if (!badAt.isEmpty()) {
			Map<Integer, List<Integer>> clusters1 = clustering.getClusters();
			Map<Integer, List<Integer>> clusters2 = basic.getClusters();
			boolean unko = false;
			for (Entry<Integer, List<Integer>> e: clusters1.entrySet()) {
				if (!(clusters2.containsKey(e.getKey()) && clusters2.get(e.getKey()).contains(e.getValue()))) {
					unko = true;
					break;
				}
			}
			if (!unko)
				return;
			System.err.println(sb.toString());
			System.err.print("Unko:");
			for (int bad: badAt) {
				System.err.print(" " + bad);
			}
			System.err.println();
		}
	}

	public static void testBottomupClusteringSimple() throws Exception {
		List<Vector> vecs = new ArrayList<Vector>();
		vecs.add(new DenseVector(new double[] {0}));
		vecs.add(new DenseVector(new double[] {2}));
		vecs.add(new DenseVector(new double[] {4}));
		vecs.add(new DenseVector(new double[] {6}));
		vecs.add(new DenseVector(new double[] {8}));
		vecs.add(new DenseVector(new double[] {10}));

		CachedBottomupClustering clustering = new CachedBottomupClustering(vecs, 1024 * 1024);
		NaiveBottomupClustering basic = new NaiveBottomupClustering(vecs);
		int i = 0;
		while (true) {
			int [] nextPair1 = clustering.popMostSimilarClusterPair();
			int [] nextPair2 = basic.next();
			if (nextPair1 == null && nextPair2 == null) {
				break;
			}
			boolean ok = (nextPair1[0] == nextPair2[0]) && (nextPair1[1] == nextPair2[1]);
			System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1] + " = " + clustering.getMaxSimilarity() + ", " 
								+ nextPair2[0] + ", " + nextPair2[1] + " = " + basic.currentMinS + " (" + ok + ")");
			++i;
		}
	}

	public static void testIndexBottomupClusteringSimple() throws Exception {
		List<Vector> vecs = new ArrayList<Vector>();
		vecs.add(new DenseVector(new double[] {0.0}));
		vecs.add(new DenseVector(new double[] {2.0}));
		vecs.add(new DenseVector(new double[] {4.0}));
		vecs.add(new DenseVector(new double[] {6.0}));
		vecs.add(new DenseVector(new double[] {8.0}));
		vecs.add(new DenseVector(new double[] {10.0}));

		IndexBottomupClustering clustering = new IndexBottomupClustering(vecs, 0);
		NaiveBottomupClustering basic = new NaiveBottomupClustering(vecs);
		int i = 0;
		while (true) {
			int [] nextPair1 = clustering.popMostSimilarClusterPair();
			int [] nextPair2 = basic.next();
			if (nextPair1 == null && nextPair2 == null) {
				break;
			}
			boolean ok = (nextPair1[0] == nextPair2[0]) && (nextPair1[1] == nextPair2[1]);
			System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1] + " = " + clustering.getMaxSimilarity() + ", " 
								+ nextPair2[0] + ", " + nextPair2[1] + " = " + basic.currentMinS + " (" + ok + ")");
			++i;
		}
	}

	public static void testIndexBottomupClustering() throws Exception {
		List<Vector> vecs = new ArrayList<Vector>();
		int nump = 300;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < nump; i++) {
			double d = Math.random();
			Vector vec = new DenseVector(new double[] {d});
			vecs.add(vec);
			sb.append("vecs.add(new DenseVector(new double[] {").append(d).append("}));\n");
		}

		IndexBottomupClustering clustering = new IndexBottomupClustering(vecs, 0);
		NaiveBottomupClustering basic = new NaiveBottomupClustering(vecs);
		int i = 0;
		while (true) {
			int [] nextPair1 = clustering.popMostSimilarClusterPair();
			int [] nextPair2 = basic.next();
			if (nextPair1 == null && nextPair2 == null) {
				break;
			}
			boolean ok = (nextPair1[0] == nextPair2[0]) && (nextPair1[1] == nextPair2[1]);
			System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1] + " = " + clustering.getMaxSimilarity() + ", " 
								+ nextPair2[0] + ", " + nextPair2[1] + " = " + basic.currentMinS + " (" + ok + ")");
			++i;
		}
	}

	public static void testIndexBottomupClusteringMany(boolean showIterations) throws Exception {
		for (int j = 0; j < 100; j++) {
			System.out.println("iteration #" + j);
			List<Vector> vecs = new ArrayList<Vector>();
			int nump = 300;
	
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < nump; i++) {
				double d = Math.random();
				Vector vec = new DenseVector(new double[] {d});
				vecs.add(vec);
				sb.append("vecs.add(new DenseVector(new double[] {").append(d).append("}));\n");
			}
	
			IndexBottomupClustering clustering = new IndexBottomupClustering(vecs, 0);
			NaiveBottomupClustering basic = new NaiveBottomupClustering(vecs);
			int i = 0;
			List<Integer> badAt = new ArrayList<Integer>();
			while (true) {
				int [] nextPair1 = clustering.popMostSimilarClusterPair();
				int [] nextPair2 = basic.next();
				if (nextPair1 == null && nextPair2 == null) {
					break;
				}
				boolean ok = (nextPair1[0] == nextPair2[0]) && (nextPair1[1] == nextPair2[1]);
				if (showIterations) {
					System.out.println("Iteration #" + i + ": " + nextPair1[0] + ", " + nextPair1[1] + " =" + clustering.getMaxSimilarity() + ", " 
										+ nextPair2[0] + ", " + nextPair2[1] + " = " + basic + " (" + ok + ")");
				}
				if (!ok) {
					badAt.add(i);
				}
				++i;
			}
			if (!badAt.isEmpty()) {
				Map<Integer, List<Integer>> clusters1 = clustering.getClusters();
				Map<Integer, List<Integer>> clusters2 = basic.getClusters();
				boolean unko = false;
				for (Entry<Integer, List<Integer>> e: clusters1.entrySet()) {
					if (!(clusters2.containsKey(e.getKey()) && clusters2.get(e.getKey()).contains(e.getValue()))) {
						unko = true;
						break;
					}
				}
				if (!unko)
					return;
				System.err.println(sb.toString());
				System.err.print("Unko:");
				for (int bad: badAt) {
					System.err.print(" " + bad);
				}
				System.err.println();
			}
		}
	}
}
