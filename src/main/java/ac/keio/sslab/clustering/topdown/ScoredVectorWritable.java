package ac.keio.sslab.clustering.topdown;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ScoredVectorWritable implements Writable {

	private final VectorWritable vectorWritable = new VectorWritable();
	private int score;

	public ScoredVectorWritable() {
	}

	public ScoredVectorWritable(int score, Vector vector) {
		this.vectorWritable.set(vector);
		this.score = score;
	}

	public Vector getVector() {
		return vectorWritable.get();
	}

	public void setVector(Vector vector) {
		vectorWritable.set(vector);
	}

	public void setScore(int score) {
		this.score = score;
	}

	public int getScore() {
		return score;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		vectorWritable.readFields(in);
		score = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vectorWritable.write(out);
		out.writeInt(score);
	}

	@Override
	public String toString() {
		Vector vector = vectorWritable.get();
		return score
				+ ": "
				+ (vector == null ? "null" : AbstractCluster.formatVector(
						vector, null));
	}

}
