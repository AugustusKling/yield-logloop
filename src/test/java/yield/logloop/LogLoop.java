package yield.logloop;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import yield.config.ConfigReader;
import yield.config.TypedYielder;
import yield.core.EventQueue;
import yield.json.JsonEvent;
import yield.logloop.function.LogloopOutput;

public class LogLoop {
	@Test
	public void writeDummies() throws InterruptedException {
		EventQueue<JsonEvent> inputQueue = new EventQueue<>();
		TypedYielder input = TypedYielder.wrap(JsonEvent.class.getName(),
				inputQueue);
		Map<String, TypedYielder> context = new HashMap<>();
		context.put(ConfigReader.LAST_SOURCE, input);

		new LogloopOutput().getSource("", context);

		JsonEvent dummy = new JsonEvent();
		dummy.put("message", "This is a dummy event from a yield.logloop test.");
		inputQueue.feed(dummy);

		// Indexers poll for events every second.
		Thread.sleep(3000);
	}
}
