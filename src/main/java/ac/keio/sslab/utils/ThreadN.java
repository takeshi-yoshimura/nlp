package ac.keio.sslab.utils;

public class ThreadN<P> implements Runnable {
	public final int numCore = Runtime.getRuntime().availableProcessors();
	public int N;
	public P parameter; // use this as parameter (List<Object> allows any kinds of parameters)

	public ThreadN(P parameter) { this.parameter = parameter; }
	public void run() {}
}