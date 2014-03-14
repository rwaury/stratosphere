package eu.stratosphere.pact.runtime.hash;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.pact.runtime.hash.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.test.util.UniformIntPairGenerator;
import eu.stratosphere.pact.runtime.test.util.types.IntPair;
import eu.stratosphere.pact.runtime.test.util.types.IntPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairPairComparator;
import eu.stratosphere.pact.runtime.test.util.types.IntPairSerializer;
import eu.stratosphere.util.MutableObjectIterator;

public class HashTablePerformanceComparisonTest {

private static final long RANDOM_SEED = 76518743207143L;
	
	private static final int KEY_VALUE_DIFF = 1021;
	
	private static final int PAGE_SIZE = 16 * 1024;
	
	private final int NUM_PAIRS = 200000;
	
	private final Random rnd = new Random(RANDOM_SEED);
		
	private final TypeSerializer<IntPair> serializer = new IntPairSerializer();
	
	private final TypeComparator<IntPair> comparator = new IntPairComparator();
	
	private final TypePairComparator<IntPair, IntPair> pairComparator = new IntPairPairComparator();
	
	private IOManager ioManager = new IOManager();

	
	@Test
	public void testCompactingHashMapPerformance() {
		
		try {
			final int NUM_MEM_PAGES = 45 * NUM_PAIRS / PAGE_SIZE;
			
			final IntPair[] pairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			final IntPair[] overwritePairs = getRandomizedIntPairs(NUM_PAIRS, rnd);
			
			long start = 0L;
			long end = 0L;
			
			System.out.println("Creating and filling CompactingHashMap...");
			start = System.currentTimeMillis();
			CompactingHashTable<IntPair> table = new CompactingHashTable<IntPair>(serializer, comparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE));
			table.open();
			end = System.currentTimeMillis();
			
			start = System.currentTimeMillis();
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insert(pairs[i]);
			}
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");
	
			CompactingHashTable<IntPair>.HashTableProber<IntPair> prober = table.createProber(comparator.duplicate(), pairComparator);
			IntPair target = new IntPair();
			
			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertTrue(prober.getMatchFor(pairs[i], target));
				assertEquals(pairs[i].getValue(), target.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			IntPair tempHolder = new IntPair();
			for (int i = 0; i < NUM_PAIRS; i++) {
				table.insertOrReplaceRecord(overwritePairs[i], tempHolder);
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");
			
			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			for (int i = 0; i < NUM_PAIRS; i++) {
				assertTrue(prober.getMatchFor(overwritePairs[i], target));
				assertEquals(overwritePairs[i].getValue(), target.getValue());
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreeMemory().size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Error: " + e.getMessage());
		}
	}
	
	@Test
	public void testMutableHashMapPerformance() {
		try {
			final int NUM_MEM_PAGES = 45 * NUM_PAIRS / PAGE_SIZE;
			
			long start = 0L;
			long end = 0L;
			
			MutableObjectIterator<IntPair> buildInput = new UniformIntPairGenerator(NUM_PAIRS, 1, false);

			MutableObjectIterator<IntPair> probeInput = new UniformIntPairGenerator(0, 1, false);
			
			System.out.println("Creating and filling MutableHashMap...");
			start = System.currentTimeMillis();
			MutableHashTable<IntPair, IntPair> table = new MutableHashTable<IntPair, IntPair>(serializer, serializer, comparator, comparator, pairComparator, getMemory(NUM_MEM_PAGES, PAGE_SIZE), ioManager);				
			table.open(buildInput, probeInput);
			end = System.currentTimeMillis();
			System.out.println("HashMap ready. Time: " + (end-start) + " ms");

			MutableObjectIterator<IntPair> probeTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			IntPair compare = new IntPair();
			
			System.out.println("Starting first probing run...");
			start = System.currentTimeMillis();
			HashBucketIterator<IntPair, IntPair> iter;
			IntPair target = new IntPair(); 
			while(probeTester.next(compare)) {
				iter = table.getMatchesFor(compare);
				iter.next(target);
				assertEquals(target.getKey(), compare.getKey());
				assertEquals(target.getValue(), compare.getValue());
				assertFalse(iter.next(target));
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
	
			MutableObjectIterator<IntPair> updater = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			System.out.println("Starting update...");
			start = System.currentTimeMillis();
			while(updater.next(compare)) {
				compare.setValue(compare.getValue()*-1);
				iter = table.getMatchesFor(compare);
				iter.next(target);
				iter.writeBack(compare);
				//assertFalse(iter.next(target));
			}
			end = System.currentTimeMillis();
			System.out.println("Update done. Time: " + (end-start) + " ms");
			
			MutableObjectIterator<IntPair> updateTester = new UniformIntPairGenerator(NUM_PAIRS, 1, false);
			System.out.println("Starting second probing run...");
			start = System.currentTimeMillis();
			while(updateTester.next(compare)) {
				compare.setValue(compare.getValue()*-1);
				iter = table.getMatchesFor(compare);
				iter.next(target);
				assertEquals(target.getKey(), compare.getKey());
				assertEquals(target.getValue(), compare.getValue());
				assertFalse(iter.next(target));
			}
			end = System.currentTimeMillis();
			System.out.println("Probing done. Time: " + (end-start) + " ms");
			
			table.close();
			assertEquals("Memory lost", NUM_MEM_PAGES, table.getFreedMemory().size());
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
