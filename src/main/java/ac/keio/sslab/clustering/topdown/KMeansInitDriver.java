package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class KMeansInitDriver {

	static int numClusterDivision = 2;
	static int defaultNumPointsForCentroidCalc = 10;

	static public void run(Configuration conf, Path input, Path output) 
					throws IOException,	ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "KMeans Init over input:" + input);
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(KMeansInitMapper.class);
		job.setReducerClass(KMeansInitReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ScoredVectorWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ClusterWritable.class);

		SideData.setNumClusterDivision(job.getConfiguration(),
				numClusterDivision);
		SideData.setDefaultNumPointsForCentroidCalc(job.getConfiguration(),
				defaultNumPointsForCentroidCalc);

		job.setJarByClass(KMeansInitMapper.class);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
	}

	static public class SideData {
		static final String prefix = "jp.ac.keio.ics.sslab.yoshimura.KMeansInit";

		static final String numClusterDivision = prefix + "numClusterDivision";

		static public void setNumClusterDivision(Configuration conf, int value) {
			conf.setInt(numClusterDivision, value);
		}

		static public int getNumClusterDivision(Configuration conf) {
			return Integer.parseInt(conf.get(numClusterDivision));
		}

		static final String defaultNumPointsForCentroidCalc = prefix
				+ "defaultNumPointsForCentroidCalc";

		static public void setDefaultNumPointsForCentroidCalc(
				Configuration conf, int value) {
			conf.setInt(defaultNumPointsForCentroidCalc, value);
		}

		static public int getDefaultNumPointsForCentroidCalc(Configuration conf) {
			return Integer.parseInt(conf.get(defaultNumPointsForCentroidCalc));
		}
	}
}
