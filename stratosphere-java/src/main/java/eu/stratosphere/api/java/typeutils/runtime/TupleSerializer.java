/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


public final class TupleSerializer<T extends Tuple> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	
	private final Class<T> tupleClass;
	
	private final TypeSerializer<Object>[] fieldSerializers;
	
	private final int arity;
	
	
	@SuppressWarnings("unchecked")
	public TupleSerializer(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
		this.tupleClass = tupleClass;
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.arity = fieldSerializers.length;
	}
	
	
	@Override
	public T createInstance() {
		try {
			T t = tupleClass.newInstance();
		
			for (int i = 0; i < arity; i++) {
				t.setField(fieldSerializers[i].createInstance(), i);
			}
			
			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate tuple.", e);
		}
	}

	@Override
	public T copy(T from, T reuse) {
		for (int i = 0; i < arity; i++) {
			Object copy = fieldSerializers[i].copy(from.getField(i), reuse.getField(i));
			reuse.setField(copy, i);
		}
		
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object o = value.getField(i);
			fieldSerializers[i].serialize(o, target);
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		for (int i = 0; i < arity; i++) {
			Object field = fieldSerializers[i].deserialize(reuse.getField(i), source);
			reuse.setField(field, i);
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		for (int i = 0; i < arity; i++) {
			fieldSerializers[i].copy(source, target);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null){
			return false;
		}
		
		if(!(obj instanceof TupleSerializer<?>)){
			return false;
		}
		
		TupleSerializer<?> otherTS = (TupleSerializer<?>) obj;
		
		return (otherTS.tupleClass == this.tupleClass) && 
				Arrays.deepEquals(this.fieldSerializers,fieldSerializers);
	}
}
