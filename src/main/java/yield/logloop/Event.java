package yield.logloop;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import yield.logloop.jdbi.TypedSQLEception;
import yield.logloop.jdbi.SyntaxErrorOrAccessRuleViolation.UndefinedTableException;

/**
 * Log event. This is stored in a daily child relation of the logloop.events
 * relation. The child relations are automatically created upon first use.
 */
public class Event {
	public final Timestamp timestamp;
	public final String message;
	public final Map<String, String> fields;
	/**
	 * Unique event identifier.
	 */
	public final UUID uuid;
	private int retention;

	public Event(UUID uuid, Timestamp timestamp, String message,
			Map<String, String> fields, int retention) {
		this.uuid = uuid;
		this.timestamp = timestamp;
		this.message = message;
		this.fields = fields;
		this.retention = retention;
	}

	/**
	 * Persists the event.
	 */
	public void store(DBI dbi) {
		try (Handle h = dbi.open()) {
			try {
				String relation = getRelation();
				Update query = h
						.createStatement("insert into "
								+ relation
								+ " (uuid, timestamp, message, fields) values (#uuid, #timestamp, #message, #fields);");
				query.bind("uuid", uuid);
				query.bind("timestamp", timestamp);
				query.bind("message", message);
				query.bind("fields", fields);
				query.execute();
			} catch (UnableToExecuteStatementException e) {
				if (TypedSQLEception.type(e) instanceof UndefinedTableException) {
					// Attempt to create the daily child relation.
					createRelation(dbi);
					// Retry to persist.
					store(dbi);
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * Creates a child relation of logloop.events to group the events by day.
	 */
	private void createRelation(DBI dbi) {
		final String relation = getRelation();

		Calendar time = Calendar.getInstance();
		time.setTimeInMillis(timestamp.getTime());

		final Calendar dayBegin = (Calendar) time.clone();
		dayBegin.set(Calendar.HOUR_OF_DAY, 0);
		dayBegin.set(Calendar.MINUTE, 0);
		dayBegin.set(Calendar.SECOND, 0);
		dayBegin.set(Calendar.MILLISECOND, 0);

		final Calendar dayEnd = (Calendar) dayBegin.clone();
		dayEnd.add(Calendar.DATE, 1);

		dbi.inTransaction(new TransactionCallback<Void>() {

			@Override
			public Void inTransaction(Handle conn, TransactionStatus status)
					throws Exception {
				// Create the relation for the daily events.
				conn.createStatement(
						"CREATE TABLE " + relation + "(" + "  CONSTRAINT pk_"
								+ getRelationName()
								+ "_uuid PRIMARY KEY (uuid),"
								+ "  CONSTRAINT chk_" + getRelationName()
								+ "_timestamp CHECK (\"timestamp\" >= '"
								+ toYMD(dayBegin)
								+ "'::date AND \"timestamp\" < '"
								+ toYMD(dayEnd) + "'::date)" + ")"
								+ " INHERITS (logloop.events)").execute();
				conn.createStatement(
						"CREATE INDEX ix_" + getRelationName()
								+ "_timestamp ON " + relation
								+ " USING btree (\"timestamp\");").execute();

				// Record daily events table for easier cleanup of outdated
				// days.
				Update schema = conn
						.createStatement("insert into logloop.events_schema (name, exclusive_upper_bound)"
								+ " values (#name, #exclusive_upper_bound::timestamp with time zone);");
				schema.bind("name", getRelationName());
				schema.bind("exclusive_upper_bound", toYMD(dayEnd));
				schema.execute();

				Update cleanup = conn
						.createStatement("select logloop.discard_older_than(#retention);");
				cleanup.bind("retention", Event.this.retention);
				cleanup.execute();

				return null;
			}
		});
	}

	private String toYMD(Calendar time) {
		return time.get(Calendar.YEAR) + "-" + (time.get(Calendar.MONTH) + 1)
				+ "-" + time.get(Calendar.DATE);
	}

	/**
	 * @return Name of daily child relation with schema.
	 */
	private String getRelation() {
		return "logloop." + getRelationName();
	}

	/**
	 * @return Name of daily child relation without schema.
	 */
	private String getRelationName() {
		Calendar time = Calendar.getInstance();
		time.setTimeInMillis(timestamp.getTime());
		return "events_y" + time.get(Calendar.YEAR) + "m"
				+ (time.get(Calendar.MONTH) + 1) + "d"
				+ time.get(Calendar.DATE);
	}

}
