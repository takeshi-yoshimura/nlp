package ac.keio.sslab.clustering.topdown;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.clustering.Model;
import org.apache.mahout.clustering.iterator.DistanceMeasureCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TopDownKMeansCluster extends DistanceMeasureCluster {

	private double RSSk;

	private boolean converged;

	/** For (de)serialization as a Writable */
	public TopDownKMeansCluster() {
	}

	public TopDownKMeansCluster(int clusterId) {
		this.setId(clusterId);
		this.setMeasure(new SquaredEuclideanDistanceMeasure());
		RSSk = 0;
		converged = false;
	}

	/**
	 * Construct a new cluster with the given point as its center
	 * 
	 * @param center
	 *            the Vector center
	 * @param clusterId
	 *            the int cluster id
	 * @param ignore
	 *            a DistanceMeasure (not used in KMeansClsuter)
	 */
	public TopDownKMeansCluster(Vector center, int clusterId,
			DistanceMeasure ignore) {
		super(center, clusterId, new SquaredEuclideanDistanceMeasure());
		RSSk = 0;
		converged = false;
	}

	/**
	 * Format the cluster for output
	 * 
	 * @param kMeansCluster
	 *            the Cluster
	 * @return the String representation of the Cluster
	 */
	public static String formatCluster(TopDownKMeansCluster cluster) {
		return cluster.getIdentifier() + ": "
				+ cluster.computeCentroid().asFormatString();
	}

	public String asFormatString() {
		return formatCluster(this);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//for avoiding a f**k bug: null dereference at serialize
		if (getRadius() == null) {
			if (getCenter() == null) {
				setCenter(computeCentroid());
			}
			setRadius(getCenter());
		}
		super.write(out);
		out.writeDouble(RSSk);
		out.writeBoolean(converged);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.RSSk = in.readDouble();
		this.converged = in.readBoolean();
	}

	@Override
	public String toString() {
		return asFormatString(null);
	}

	@Override
	public String getIdentifier() {
		return (converged ? "VL-" : "CL-") + getId();
	}

	@Override
	public void observe(Model<VectorWritable> x) {
		super.observe(x);
		if (x instanceof TopDownKMeansCluster) {
			TopDownKMeansCluster c1 = (TopDownKMeansCluster) x;
			this.RSSk += c1.RSSk;
		}
	}
	
	public void setRSSk(double RSSk) {
		this.RSSk = RSSk;
	}

	public double getRSSk() {
		return this.RSSk;
	}

	public void incrementRSSk(Vector x) {
		RSSk += this.getMeasure().distance(x, getCenter());
	}

	public boolean isConverged() {
		return this.converged;
	}

	public void setConverged(boolean converged) {
		this.converged = converged;
	}

}
