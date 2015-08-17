package ac.keio.sslab.utils;

import java.util.ArrayList;
import java.util.List;

public class SymmetricParallelMethod {

	public static <P, T extends ThreadN<P>> void runAndWait(Class<T> clazz, Class<P> pclazz, P parameter) throws Exception {
		List<Thread> list = new ArrayList<Thread>();
		for (int n = 0; n < Runtime.getRuntime().availableProcessors(); n++) {
			list.add(new Thread(clazz.getConstructor(pclazz).newInstance(parameter)));
			list.get(n).start();
		}

		for (Thread t: list) {
			t.join();
		}
	}
}
