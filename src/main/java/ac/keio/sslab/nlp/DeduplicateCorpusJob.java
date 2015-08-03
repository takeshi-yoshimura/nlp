package ac.keio.sslab.nlp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;

public class DeduplicateCorpusJob implements NLPJob {

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
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("c", "corpusID", true, "ID of a corpus"));
		g.setRequired(true);

		Options opts = new Options();
		opts.addOptionGroup(g);
		return opts;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		Path corpusPath = mgr.getArgJobIDPath(conf.corpusPath, "c");
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		Configuration hdfsConf = new Configuration();
		try {
			Map<String, String> out = new HashMap<String, String>();//<value, key> in original corpus
			SequenceDirectoryReader<String, String> reader = new SequenceDirectoryReader<>(corpusPath, hdfsConf, String.class, String.class);
			while (reader.seekNext()) {
				out.put(reader.val(), reader.key());
			}
			reader.close();

			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, hdfsConf, mgr.doForceWrite(), String.class, String.class);
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
