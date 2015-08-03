package ac.keio.sslab.clustering.bottomup;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public interface BottomupClusteringListener {
	public boolean next();
	public int mergingPointId();
	public int mergedPointId();
	public Vector newPointVector();
	public DistanceMeasure getDistanceMeasure();
}
