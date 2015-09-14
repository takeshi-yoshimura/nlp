package ac.keio.sslab.analytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.job.JobUtils;

public class HierarchicalCluster {
	HierarchicalCluster parentC, leftC, rightC;
	int ID;
	List<Integer> points;
	double density;
	Map<String, Double> centroid;

	public HierarchicalCluster(HierarchicalCluster leftC, HierarchicalCluster rightC, int ID) {
		this.parentC = null;
		this.leftC = leftC;
		this.leftC.parentC = this;
		this.rightC = rightC;
		this.rightC.parentC = this;
		this.points = new ArrayList<Integer>();
		this.points.addAll(leftC.points);
		this.points.addAll(rightC.points);
		this.ID = ID;
	}

	public HierarchicalCluster(int pointID, int ID) {
		this.parentC = this.leftC = this.rightC = null;
		this.points = new ArrayList<Integer>();
		this.points.add(pointID);
		this.ID = ID;
		this.density = 0;
	}

	public HierarchicalCluster(int ID) {
		this.ID = ID;
	}

	public int getID() {
		return ID;
	}

	public HierarchicalCluster getParent() {
		return parentC;
	}

	public void setParent(HierarchicalCluster c) {
		parentC = c;
	}

	public HierarchicalCluster getLeft() {
		return leftC;
	}

	public void setLeft(HierarchicalCluster c) {
		leftC = c;
	}

	public HierarchicalCluster getRight() {
		return rightC;
	}

	public void setRight(HierarchicalCluster c) {
		rightC = c;
	}

	public int size() {
		return points.size();
	}

	public List<Integer> getPoints() {
		return points;
	}

	public void setPoints(List<Integer> points) {
		this.points = points;
	}

	public Vector getCentroid(List<Vector> pointVectors) {
		Vector centroid = new DenseVector(pointVectors.get(this.points.get(0)).size());
		for (int point: points) {
			centroid = centroid.plus(pointVectors.get(point));
		}
		return centroid.divide(points.size());			
	}

	public void setCentroid(List<Vector> pointVectors, Map<Integer, String> topicStr) {
		centroid = new HashMap<String, Double>();
		for (Entry<Integer, Double> e: JobUtils.getTopElements(getCentroid(pointVectors), 10)) {
			centroid.put((topicStr == null ? Integer.toString(e.getKey()): topicStr.get(e.getKey())), e.getValue());
		}
	}

	public void setCentroid(Map<String, Double> str) {
		centroid = str;
	}

	public Map<String, Double> getCentroid() {
		return centroid;
	}

	// currently, use the average distance to each other as the density of a HierarchicalCluster
	public void setDensity(double [][] distance) {
		if (points.size() == 1) {
			density = 0.0;
		}
		double density = 0;
		for (int i: points) {
			for (int j: points) {
				if (i > j) {
					density += distance[i][j];
				}
			}
		}
		this.density = density / (points.size() * (points.size() - 1) / 2);
	}

	public void setDensity(double density) {
		this.density = density;
	}

	public double getDensity() {
		return density;
	}

	@Override
	public String toString() {
		return toString(Integer.toString(ID));
	}

	public String toString(String realID) {
		StringBuilder sb = new StringBuilder();
		sb.append(realID).append(',');
		sb.append(size()).append(',');
		sb.append(getDensity()).append(',');
		sb.append(parentC != null ? parentC.ID: -1).append(',');
		sb.append(leftC != null ? leftC.ID: -1).append(',');
		sb.append(rightC != null ? rightC.ID: -1).append(',');
		for (Entry<String, Double> e: getCentroid().entrySet()) {
			sb.append(e.getKey()).append(',').append(e.getValue()).append(',');
		}
		for (int p: points) {
			sb.append(p).append(',');
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public static HierarchicalCluster parseString(String str) {
		String [] s = str.split(",");
		int i = 0;
		int ID = Integer.parseInt(s[i++]);
		int size = Integer.parseInt(s[i++]);
		double density = Double.parseDouble(s[i++]);
		int parentCID = Integer.parseInt(s[i++]);
		int leftCID = Integer.parseInt(s[i++]);
		int rightCID = Integer.parseInt(s[i++]);

		Map<String, Double> centroidStr = new HashMap<String, Double>();
		for (; i < s.length - size; i += 2) {
			centroidStr.put(s[i], Double.parseDouble(s[i + 1]));
		}

		List<Integer> points = new ArrayList<Integer>();
		for (; i < s.length; i++) {
			points.add(Integer.parseInt(s[i]));
		}

		HierarchicalCluster parentC = parentCID != -1 ? new HierarchicalCluster(parentCID): null;
		HierarchicalCluster leftC = leftCID != -1 ? new HierarchicalCluster(leftCID): null;
		HierarchicalCluster rightC = rightCID != -1 ? new HierarchicalCluster(rightCID): null;
		HierarchicalCluster c = new HierarchicalCluster(ID);
		c.setLeft(leftC);
		c.setRight(rightC);
		c.setParent(parentC);
		c.setDensity(density);
		c.setPoints(points);
		c.setCentroid(centroidStr);
		return c;
	}
}
