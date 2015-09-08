package ac.keio.sslab.nlp;

public abstract class SingletonGroupNLPJob implements NLPJob, NLPJobGroup {

	@Override
	public String getAlgorithmName() {
		return getJobName();
	}

	@Override
	public String getAlgorithmDescription() {
		return getJobDescription();
	}

	@Override
	public NLPJobGroup getJobGroup() {
		return this;
	}
}
