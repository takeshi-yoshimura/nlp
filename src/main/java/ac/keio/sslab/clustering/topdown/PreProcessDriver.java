package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

public class PreProcessDriver {

	static public void run(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "PreProcess over input:" + input);
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(PreProcessMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);

		job.setJarByClass(PreProcessMapper.class);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
	}
}
