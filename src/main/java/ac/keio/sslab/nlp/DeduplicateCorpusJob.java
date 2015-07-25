package ac.keio.sslab.nlp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;

public class DeduplicateCorpusJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();
	@Override
	public String getJobName() {
		return "dedupCorpus";
	}

	@Override
	public String getJobDescription() {
		return "remove identical documents";
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption("c", "corpusID", true, "ID of a corpus");
		return opts;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("c")) {
			System.err.println("Need to specify --corpusID");
			return;
		}
		Path corpusPath = new Path(conf.corpusPath, args.get("c"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		Configuration hdfsConf = new Configuration();
		try {
			Map<String, String> out = new HashMap<String, String>();//<value, key> in original corpus
			SequenceDirectoryReader<String, String> reader = new SequenceDirectoryReader<>(corpusPath, hdfsConf);
			while (reader.seekNext()) {
				out.put(reader.val(), reader.key());
			}
			reader.close();

			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, hdfsConf, args.containsKey("ow"));
			for (Entry<String, String> e: out.entrySet()) {
				writer.append(e.getValue(), e.getKey());
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Reading or writing to HDFS failed: " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
