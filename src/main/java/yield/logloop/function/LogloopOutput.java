package yield.logloop.function;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import yield.config.ConfigReader;
import yield.config.FunctionConfig;
import yield.config.ParameterMap;
import yield.config.ParameterMap.Param;
import yield.config.ShortDocumentation;
import yield.config.TypedYielder;
import yield.core.Yielder;
import yield.json.JsonEvent;

public class LogloopOutput extends FunctionConfig {
	private static enum Parameters implements Param {
		@ShortDocumentation(text = "JDBC connection URI.")
		connectionURI {
			@Override
			public Object getDefault() {
				return "jdbc:postgresql://localhost/logloop?user=logloop&password=logloop&ssl=true";
			}
		},
		@ShortDocumentation(text = "Number of thread to write received events to database.")
		writerThreads {
			@Override
			public Object getDefault() {
				return 2;
			}
		},
		@ShortDocumentation(text = "Number of days that events shall be retained. Older events are discarded.")
		retention {
			@Override
			public Object getDefault() {
				return 5;
			}
		}
	}

	@SuppressWarnings("null")
	@Override
	@Nonnull
	public TypedYielder getSource(String args, Map<String, TypedYielder> context) {
		ParameterMap<Parameters> parameters = parseArguments(args,
				Parameters.class);
		int writers = parameters.getInteger(Parameters.writerThreads);
		String connectionURI = parameters.getString(Parameters.connectionURI);
		int retention = parameters.getInteger(Parameters.retention);
		yield.logloop.LogloopOutput output = new yield.logloop.LogloopOutput(
				connectionURI, writers, retention);

		Yielder<JsonEvent> input = getYielderTypesafe(JsonEvent.class,
				ConfigReader.LAST_SOURCE, context);
		input.bind(output);
		return context.get(ConfigReader.LAST_SOURCE);
	}

	@Override
	@Nullable
	public <Parameter extends Enum<Parameter> & Param> Class<? extends Param> getParameters() {
		return Parameters.class;
	}
}
