package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class CalcConvergenceDriver {

	static float convergenceDelta = 0.01f;
	
	static public long run(Configuration conf, Path input, Path output) 
					throws IOException, ClassNotFoundException,
			InterruptedException {
		Job job = new Job(conf, "Calculate convergence over input:" + input);
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(CalcConvergenceMapper.class);
		job.setReducerClass(CalcConvergenceReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ClusterWritable.class);
		SequenceFileOutputFormat.setOutputPath(job, output);

		SideData.setConvergenceDelta(job.getConfiguration(), convergenceDelta);

		job.setJarByClass(CalcConvergenceMapper.class);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
		
		Counter counter = job.getCounters().findCounter(ConvergenceCounter.unconverged);
		return counter.getValue();
	}
	
	//clusters must be sorted by RSSSorter
	static boolean isConverged(List<TopDownKMeansCluster> clusters, float convergenceDelta) {
		//clusters are converged if their centroids are not moved
		if (clusters.get(0).getCenter().equals(clusters.get(1).getCenter()))
			return true;

		//clusters are converged if d^2RSS is enough small
		double RSS2 = clusters.get(2).getRSSk();
		double RSS1 = clusters.get(1).getRSSk();
		double RSS0 = clusters.get(0).getRSSk();
		if (RSS2 - RSS1 <= (1.00f + convergenceDelta) * (RSS1 - RSS0)) {
			return true;
		}
		return false;
	}

	static public class RSSSorter implements Comparator<TopDownKMeansCluster> {

		@Override
		public int compare(TopDownKMeansCluster o1, TopDownKMeansCluster o2) {
			double r1 = o1.getRSSk();
			double r2 = o2.getRSSk();
			return r1 > r2 ? 1: (r1 == r2 ? 0: -1);
		}
	}
	
	static public enum ConvergenceCounter {
		unconverged
	}

	static public class SideData {
		static final String prefix = "jp.ac.keio.ics.sslab.yoshimura.calcconvergence.";

		static final String convergenceDelta = prefix + "convergenceDelta";

		static public void setConvergenceDelta(Configuration conf, float value) {
			conf.setFloat(convergenceDelta, value);
		}
		static public float getConvergenceDelta(Configuration conf) {
			return Float.parseFloat(conf.get(convergenceDelta));
		}
	}
}
