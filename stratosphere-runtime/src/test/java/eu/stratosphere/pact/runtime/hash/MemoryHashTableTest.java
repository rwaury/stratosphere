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
package eu.stratosphere.pact.runtime.hash;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairSerializer;

import static org.junit.Assert.*;


public class MemoryHashTableTest {
	
	private static final long RANDOM_SEED = 76518743207143L;
	
	private static final int KEY_VALUE_DIFF = 1021;
	
	private static final int PAGE_SIZE = 16 * 1024;
	
	
	private final Random rnd = new Random(RANDOM_SEED);
	
	private final TypeSerializer<IntPair> serializer = new IntPairSerializer();
	
	private final TypeComparator<IntPair> comparator = new IntPairComparator();
	
	private final TypePairComparator<IntPair, IntPair> pairComparator = new IntPairPairComparator();
	
	
	@Test
	public void testBuildAndRetrieve() {
		
		try {
			final int NUM_PAIRS = 100000;
			final int NUM_MEM_PAGES = 32 * NUM_PAIRS / PAGE_SIZE;
			
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			CompactingHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
	
			CompactingHashTable<IntPair>.HashTableProber<IntPair> prober = table.createProber(comparator.duplicate(), pairComparator);
			IntPair target = new IntPair();
			
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertTrue(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	
	private static IntPair[] getRandomizedIntPairs(int num, Random rnd) {
		IntPair[] pairs = new IntPair[num];
		
		// create all the pairs, dense
		for (int i = 0; i < num; i++) {
			pairs[i] = new IntPair(i, i + KEY_VALUE_DIFF);
		}
		
		// randomly swap them
		for (int i = 0; i < 2 * num; i++) {
			int pos1 = rnd.nextInt(num);
			int pos2 = rnd.nextInt(num);
			
			IntPair tmp = pairs[pos1];
			pairs[pos1] = pairs[pos2];
			pairs[pos2] = tmp;
		}
		
		return pairs;
	}
	
	private static List<MemorySegment> getMemory(int numPages, int pageSize) {
		List<MemorySegment> memory = new ArrayList<MemorySegment>();
		
		for (int i = 0; i < numPages; i++) {
			memory.add(new MemorySegment(new byte[pageSize]));
		}
		
		return memory;
	}
}
