package ac.keio.sslab.clustering.bottomup;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public interface BottomupClusteringAlgorithm {
	public boolean next();
	public Vector update();
	public int mergingPointId();
	public int mergedPointId();
	public DistanceMeasure getDistanceMeasure();
}
