package ac.keio.sslab.nlp;

import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Options;

public class SnapshotManager implements NLPJob {

	protected List<NLPJob> jobs;
	protected final NLPConf conf = new NLPConf();

	public SnapshotManager(List<NLPJob> jobs) {
		this.jobs = jobs;
	}

	@Override
	public String getJobName() {
		return NLPConf.snapshotJobName;
	}

	@Override
	public String getJobDescription() {
		return "Saves outputs that are necessary for restarting jobs from nodes to the central server";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("r", "restore", false, "Restore the snapshot");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("r")) {
			//Take a snapshot
			for (NLPJob job: jobs) {
				job.takeSnapshot();
			}
		} else {
			//Restore the snapshot
			for (NLPJob job: jobs) {
				job.restoreSnapshot();
			}
		}
	}

	@Override
	public void takeSnapshot() {
		/* do nothing */
	}

	@Override
	public void restoreSnapshot() {
		/* do nothing */
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
