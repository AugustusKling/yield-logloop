package yield.logloop;

import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.skife.jdbi.v2.DBI;

/**
 * Worker to be run in thread pool to persist events.
 */
public class WriteTask implements Runnable {
	private Queue<Event> events;
	private DBI dbi;
	private ScheduledExecutorService indexers;
	private static Logger logger = Logger.getLogger(WriteTask.class);

	public WriteTask(Queue<Event> events, DBI dbi,
			ScheduledExecutorService indexers) {
		this.events = events;
		this.dbi = dbi;
		this.indexers = indexers;
	}

	@Override
	public void run() {
		Event event = events.poll();
		if (event != null) {
			// Let worker act again when spare thread gets available in pool.
			indexers.submit(this);

			try {
				event.store(dbi);
			} catch (Exception e) {
				// Try again to write events that failed for whatever reason.
				events.add(event);

				logger.error("Failed to store event. Queueing for retry.", e);
			}
		} else {
			// When there is no work to do, idle worker for some delay to keep
			// system load low.
			indexers.schedule(this, 1, TimeUnit.SECONDS);
		}
	}

}
