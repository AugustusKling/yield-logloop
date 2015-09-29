package yield.logloop;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.HashPrefixStatementRewriter;

import yield.core.BaseControlQueueProvider;
import yield.core.EventListener;
import yield.core.EventType;
import yield.json.JsonEvent;

import com.fasterxml.jackson.databind.JsonNode;

public class LogloopOutput extends BaseControlQueueProvider implements
		EventListener<JsonEvent> {
	private static final Logger logger = LogManager
			.getLogger(LogloopOutput.class);

	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new Error("Could not load PostgreSQL driver.", e);
		}
	}
	private static DataSource dataSource;
	private static DBI dbi;

	/**
	 * Holds log events that have been read from the network but are not yet
	 * persisted.
	 */
	private static Queue<Event> events = new ConcurrentLinkedQueue<>();

	/**
	 * Pool of workers that consume the {@link #events} and persist them.
	 */
	private static ScheduledExecutorService indexers;

	/**
	 * Number of days to keep events.
	 */
	private int retention;

	public LogloopOutput(String connectionURI, int poolSize, int retention) {
		this.retention = retention;
		dataSource = setupDataSource(connectionURI, poolSize);
		indexers = Executors.newScheduledThreadPool(poolSize);
		for (int writerIndex = 0; writerIndex < poolSize; writerIndex++) {
			WriteTask writer = new WriteTask(events, getDBI(), indexers);
			indexers.submit(writer);
		}
	}

	public static DBI getDBI() {
		if (dbi == null) {
			synchronized (dataSource) {
				if (dbi == null) {
					DataSource ds = dataSource;
					dbi = new DBI(ds);
					dbi.setStatementRewriter(new HashPrefixStatementRewriter());
				}
			}
		}
		return dbi;
	}

	private static DataSource setupDataSource(String connectURI, int poolSize) {
		//
		// First, we'll create a ConnectionFactory that the
		// pool will use to create Connections.
		// We'll use the DriverManagerConnectionFactory,
		// using the connect string passed in the command line
		// arguments.
		//
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
				connectURI, null);
		//
		// Next we'll create the PoolableConnectionFactory, which wraps
		// the "real" Connections created by the ConnectionFactory with
		// the classes that implement the pooling functionality.
		//
		GenericObjectPool dbPool = new GenericObjectPool();
		dbPool.setMaxIdle(poolSize);
		dbPool.setMaxActive(poolSize);
		new PoolableConnectionFactory(connectionFactory, dbPool, null,
				"select 1", false, true);

		//
		// Finally, we create the PoolingDriver itself,
		// passing in the object pool we created.
		//
		PoolingDataSource dataSource = new PoolingDataSource(dbPool);
		return dataSource;
	}

	@Override
	public void feed(JsonEvent e) {
		Map<String, String> fields = new HashMap<>();
		Iterator<Entry<String, JsonNode>> fieldIter = e.getObject().fields();
		while (fieldIter.hasNext()) {
			Entry<String, JsonNode> entry = fieldIter.next();
			if (!entry.getKey().equals("message")) {
				fields.put(entry.getKey(), entry.getValue().textValue());
			}
		}
		// Queue as log event.
		String time = e.get("timestamp");
		Timestamp timestamp;
		if (time == null) {
			timestamp = new Timestamp(Calendar.getInstance().getTimeInMillis());
		} else {
			try {
				timestamp = new Timestamp(Long.parseLong(time));
			} catch (Exception e2) {
				logger.error("Failed to read timestamp", e2);
				timestamp = new Timestamp(Calendar.getInstance()
						.getTimeInMillis());
			}
		}
		Event event = new Event(UUID.randomUUID(), timestamp, e.get("message"),
				fields, retention);
		events.add(event);
	}

	/**
	 * Blocks until all events received so far have been persisted in the
	 * database.
	 */
	public void awaitCompletion() throws InterruptedException {
		boolean terminated = false;
		while (!(terminated || events.isEmpty())) {
			terminated = indexers.awaitTermination(1, TimeUnit.SECONDS);
		}
	}

	@Override
	@Nonnull
	public EventType getInputType() {
		return new EventType(JsonEvent.class);
	}
}
