package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.StringValue;

public class StringPairSerializer extends TypeSerializer<StringPair> {

	@Override
	public StringPair createInstance() {
		return new StringPair();
	}

	@Override
	public StringPair createCopy(StringPair from) {
		return new StringPair(from.getKey(), from.getValue());
	}

	@Override
	public void copyTo(StringPair from, StringPair to) {
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StringPair record, DataOutputView target)
			throws IOException {
		StringValue.writeString(record.getKey(), target);
		StringValue.writeString(record.getValue(), target);
	}

	@Override
	public void deserialize(StringPair record, DataInputView source)
			throws IOException {
		record.setKey(StringValue.readString(source));
		record.setValue(StringValue.readString(source));
	}

	@Override
	public void copy(DataInputView source, DataOutputView target)
			throws IOException {
		StringValue.writeString(StringValue.readString(source), target);
		StringValue.writeString(StringValue.readString(source), target);
	}

}
