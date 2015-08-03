package ac.keio.sslab.nlp;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.clustering.bottomup.BottomupClustering;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;

public class BottomUpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "hierarchical clustering";
	}

	@Override
	public Options getOptions() {
		OptionGroup required = new OptionGroup();
		required.addOption(new Option("l", "ldaID", true, "ID of lda"));
		required.addOption(new Option("d", "distanceMeasure", true, "distance measure name (Cosine or Euclidean)"));
		required.setRequired(true);

		Options opt = new Options();
		opt.addOption("t", "threashold", true, "bytes of threashold to optimize algorithm. K, M, G postfix are available (default: 6.4G)");
		opt.addOptionGroup(required);
		return opt;
	}

	public void generateInput(Path input) throws Exception {
		Configuration hdfsConf = new Configuration();
		SequenceSwapWriter<Integer, Vector> writer = new SequenceSwapWriter<Integer, Vector>(input, NLPConf.getInstance().tmpPath, hdfsConf, true, Integer.class, Vector.class);
		int card = 2;
		int nump = 20;

		for (int i = 0; i < nump; i++) {
			Vector vec = new DenseVector(card);
			for (int j = 0; j < card; j++) {
				vec.set(j, Math.random());
			}
			writer.append(i, vec);
			System.out.println(i + ": " + vec);
		}
		writer.close();
	}

	public long parseThreashold(String str) {
		long postfix = 1;
		str = str.toLowerCase();
		if (str.matches("^.*[mgk]$")) {
			switch(str.charAt(str.length() - 1)) {
			case 'k': postfix = 1024; break;
			case 'm': postfix = 1024 * 1024; break;
			case 'g': postfix = 1024 * 1024 * 1024; break;
			}
			str = str.substring(0, str.length() - 1);
		}
		double d = Double.parseDouble(str);
		return (long)(d * postfix);
	}

	@Override
	public void run(JobManager mgr) {
		long threashold = parseThreashold(mgr.getArgOrDefault("t", "6.4G", String.class));

		//LDAHDFSFiles ldaFiles = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
		//Path input = ldaFiles.docIndexPath;
		Path input = new Path(mgr.getNLPConf().tmpPath, "testBottomUp");
		try {
			generateInput(input);
			BottomupClustering bc = new BottomupClustering(input, new Configuration(), mgr.getArgStr("d"), threashold);
			while (bc.next()) {
				int merged = bc.mergedPointId();
				int merging = bc.mergingPointId();
				System.out.println(merging + " <- " + merged);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
