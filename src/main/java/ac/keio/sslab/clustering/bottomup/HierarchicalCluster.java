package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.JobUtils;

public class HierarchicalCluster {
	HierarchicalCluster parentC, leftC, rightC;
	int ID;
	List<Integer> points;
	double density;
	String centroidString;

	public HierarchicalCluster(HierarchicalCluster leftC, HierarchicalCluster rightC, int ID) {
		this.parentC = null;
		this.leftC = leftC;
		this.rightC = rightC;
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

	protected HierarchicalCluster(int ID) {
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

	public void setCentroidString(List<Vector> pointVectors, Map<Integer, String> topicStr) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		sb.append('"');
		for (Entry<Integer, Double> e: JobUtils.getTopElements(getCentroid(pointVectors), 3)) {
			if (++i > 3) {
				break;
			}
			sb.append((topicStr == null ? e.getKey(): topicStr.get(e.getKey()))).append(':').append(String.format("%1$3f", e.getValue())).append(',');
		}
		sb.setLength(sb.length() - 1);
		sb.append('"');
		centroidString = sb.toString();
	}

	public void setCentroidString(String str) {
		centroidString = str;
	}

	public String getCentroidString() {
		return centroidString;
	}

	// currently, use the average distance to each other as the density of a HierarchicalCluster
	public void setDensity(double [][] distance) {
		if (points.size() == 1) {
			return;
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
		StringBuilder sb = new StringBuilder();
		sb.append(ID).append(',');
		sb.append(size()).append(',');
		sb.append(getDensity()).append(',');
		sb.append(parentC != null ? parentC.ID: -1).append(',');
		sb.append(leftC != null ? leftC.ID: -1).append(',');
		sb.append(rightC != null ? rightC.ID: -1).append(',');
		sb.append(getCentroidString());
		for (int p: points) {
			sb.append(p).append(',');
		}
		return sb.toString();
	}

	public static HierarchicalCluster parseString(String str) {
		String [] s = str.split(",");
		int ID = Integer.parseInt(s[0]);
		int size = Integer.parseInt(s[2]);
		double density = Double.parseDouble(s[4]);
		int parentCID = Integer.parseInt(s[6]);
		int leftCID = Integer.parseInt(s[8]);
		int rightCID = Integer.parseInt(s[10]);
		HierarchicalCluster parentC = parentCID != -1 ? new HierarchicalCluster(parentCID): null;
		HierarchicalCluster leftC = leftCID != -1 ? new HierarchicalCluster(leftCID): null;
		HierarchicalCluster rightC = rightCID != -1 ? new HierarchicalCluster(rightCID): null;

		StringBuilder sb = new StringBuilder();
		int i = 12;
		for (; i < s.length - size * 2; i++) {
			sb.append(s[i]);
		}
		String centroidString = sb.toString();

		List<Integer> points = new ArrayList<Integer>();
		for (; i < s.length; i += 2) {
			points.add(Integer.parseInt(s[i]));
		}

		HierarchicalCluster c = new HierarchicalCluster(leftC, rightC, ID);
		c.setParent(parentC);
		c.setDensity(density);
		c.setPoints(points);
		c.setCentroidString(centroidString);
		return c;
	}
}
