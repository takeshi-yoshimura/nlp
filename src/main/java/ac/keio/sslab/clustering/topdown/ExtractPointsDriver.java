package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

public class ExtractPointsDriver extends AbstractJob {

	public void run(Path pointClusterDir, Path docTopicDir, Path output, int clusterID) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(getConf(), "extract clusterID" + clusterID + " over input: "
				+ pointClusterDir + " and " + docTopicDir);
		MultipleInputs.addInputPath(job, pointClusterDir,
				SequenceFileInputFormat.class, ExtractPointsMapper.class);
		MultipleInputs.addInputPath(job, docTopicDir,
				SequenceFileInputFormat.class, Mapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);

		job.setReducerClass(ExtractPointsReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		
		job.getConfiguration().setInt("jp.ac.keio.ics.sslab.yoshimura.ExtractPointsDriver.extractID", clusterID);

		job.setJarByClass(ExtractPointsMapper.class);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
	}

	@Override
	public int run(String[] args) throws Exception {
		addOutputOption();
		addOption("docTopicDir", "dt", "cvbJobDir/docTopic", true);
		addOption("finalJobDir", "f", "finalJobDir", true);
		addOption("ID", "id", "extracting this ID from input (clusteredPoints)", true);

		if (parseArguments(args) == null) {
			return -1;
		}
		Path docTopicDir = new Path(getOption("docTopicDir"));
		Path finalJobDir = new Path(getOption("finalJobDir"));
		int id = Integer.parseInt(getOption("ID"));
		int order = (int)(Math.log(id) / Math.log(2));
		Path pointClusterDir = new Path(finalJobDir, "topdown-" + order + "/point-cluster");
		run(pointClusterDir, docTopicDir, getOutputPath(), id);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ExtractPointsDriver(), args);
	}
}
