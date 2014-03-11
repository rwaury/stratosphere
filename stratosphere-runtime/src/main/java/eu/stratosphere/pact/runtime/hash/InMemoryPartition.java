/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.core.memory.MemorySegmentSource;
import eu.stratosphere.core.memory.SeekableDataInputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;

/**
 * @param BT The type of the records.
 */
public class InMemoryPartition<T> {
	
	// --------------------------------- Table Structure Auxiliaries ------------------------------------
	
	protected MemorySegment[] overflowSegments;	// segments in which overflow buckets from the table structure are stored
	
	protected int numOverflowSegments;			// the number of actual segments in the overflowSegments array
	
	protected int nextOverflowBucket;				// the next free bucket in the current overflow segment

	// -------------------------------------  Type Accessors --------------------------------------------
	
	private final TypeSerializer<T> serializer;
	
	// -------------------------------------- Record Buffers --------------------------------------------
	
	private final ArrayList<MemorySegment> partitionPages;
	
	private final ListMemorySegmentSource availableMemory;
	
	private final WriteView writeView;
	
	private final ReadView readView;
	
	private long recordCounter;				// number of build-side records in this partition 
	
	// ----------------------------------------- General ------------------------------------------------
	
	private int partitionNumber;					// the number of the partition
	
	private boolean compacted;
	
	// --------------------------------------------------------------------------------------------------
	
	
	
	/**
	 * Creates a new partition, initially in memory, with one buffer for the build side. The partition is
	 * initialized to expect record insertions for the build side.
	 * 
	 * @param partitionNumber The number of the partition.
	 * @param recursionLevel The recursion level - zero for partitions from the initial build, <i>n + 1</i> for
	 *                       partitions that are created from spilled partition with recursion level <i>n</i>. 
	 * @param initialBuffer The initial buffer for this partition.
	 * @param writeBehindBuffers The queue from which to pop buffers for writing, once the partition is spilled.
	 */
	public InMemoryPartition(TypeSerializer<T> serializer, int partitionNumber,
			ListMemorySegmentSource memSource, int pageSize, int pageSizeInBits)
	{
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		
		this.serializer = serializer;
		this.partitionPages = new ArrayList<MemorySegment>(64);
		this.availableMemory = memSource;
		
		this.partitionNumber = partitionNumber;
		
		// add the first segment
		this.partitionPages.add(memSource.nextSegment());
		this.compacted = true;
		
		this.writeView = new WriteView(this.partitionPages, memSource, pageSize, pageSizeInBits);
		this.readView = new ReadView(this.partitionPages, pageSize, pageSizeInBits);
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Gets the partition number of this partition.
	 * 
	 * @return This partition's number.
	 */
	public int getPartitionNumber() {
		return this.partitionNumber;
	}
	
	public void setPartitionNumber(int number) {
		this.partitionNumber = number;
	}
	
	public int getBlockCount() {
		return this.partitionPages.size();
	}
	
	public long getRecordCount() {
		return this.recordCounter;
	}
	
	public void resetRecordCounter() {
		this.recordCounter = 0L;
	}
	
	public boolean isCompacted() {
		return this.compacted;
	}
	
	public void setCompaction(boolean compacted) {
		this.compacted = compacted;
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Inserts the given object into the current buffer. This method returns a pointer that
	 * can be used to address the written record in this partition, if it is in-memory. The returned
	 * pointers have no expressiveness in the case where the partition is spilled.
	 * 
	 * @param object The object to be written to the partition.
	 * @return A pointer to the object in the partition, or <code>-1</code>, if the partition is spilled.
	 * @throws IOException Thrown, when this is a spilled partition and the write failed.
	 */
	public final long appendRecord(T record) throws IOException {
		long pointer = this.writeView.getCurrentPointer();
		try {
			this.serializer.serialize(record, this.writeView);
			this.recordCounter++;
			return pointer;
		}
		catch (EOFException e) {
			// we ran out of pages. 
			// first, reset the pages and then we need to trigger a compaction
			int oldCurrentBuffer = this.writeView.resetTo(pointer);
			for (int bufNum = this.partitionPages.size() - 1; bufNum > oldCurrentBuffer; bufNum--) {
				this.availableMemory.addMemorySegment(this.partitionPages.remove(bufNum));
			}
			throw e;
		}
	}
	
	public void readRecordAt(long pointer, T record) throws IOException {
		this.readView.setReadPosition(pointer);
		this.serializer.deserialize(record, this.readView);
	}
	
	/**
	 * UNSAFE!! overwrites record
	 * causes inconsistency or data loss for overwriting everything but records of the exact same size
	 * 
	 * @param pointer pointer to start of record
	 * @param record record to overwrite old one with
	 * @throws IOException
	 */
	public void overwriteRecordAt(long pointer, T record) throws IOException {
		long tmpPointer = this.writeView.getCurrentPointer();
		this.writeView.resetTo(pointer);
		this.serializer.serialize(record, this.writeView);
		this.writeView.resetTo(tmpPointer);
	}
	
	public void clearAllMemory(List<MemorySegment> target) {
		// return the overflow segments
		if (this.overflowSegments != null) {
			for (int k = 0; k < this.numOverflowSegments; k++) {
				target.add(this.overflowSegments[k]);
			}
		}
		
		// return the partition buffers
		target.addAll(this.partitionPages);
		this.partitionPages.clear();
	}
	
	public void allocateSegments(int numberOfSegments) {
		while(getBlockCount() < numberOfSegments) {
			MemorySegment next = this.availableMemory.nextSegment();
			if(next != null) {
				this.partitionPages.add(next);
			} else {
				return;
			}
		}
	}

	@Override
	public String toString() {
		return String.format("Partition %d - %d records, %d partition blocks, %d bucket overflow blocks", getPartitionNumber(), getRecordCount(), getBlockCount(), this.numOverflowSegments);
	}
	
	// ============================================================================================
	
	private static final class WriteView extends AbstractPagedOutputView {
		
		private final ArrayList<MemorySegment> pages;
		
		private final MemorySegmentSource memSource;
		
		private final int sizeBits;
		
		private final int sizeMask;
		
		private int currentPageNumber;
		
		private int segmentNumberOffset;
		
		
		private WriteView(ArrayList<MemorySegment> pages, MemorySegmentSource memSource,
				int pageSize, int pageSizeBits)
		{
			super(pages.get(0), pageSize, 0);
			
			this.pages = pages;
			this.memSource = memSource;
			this.sizeBits = pageSizeBits;
			this.sizeMask = pageSize - 1;
		}
		

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
			MemorySegment next = this.memSource.nextSegment();
			if(next == null) {
				throw new EOFException();
			}
			this.pages.add(next);
			
			this.currentPageNumber++;
			return next;
		}
		
		private long getCurrentPointer() {
			return (((long) this.currentPageNumber) << this.sizeBits) + getCurrentPositionInSegment();
		}
		
		private int resetTo(long pointer) {
			final int pageNum  = (int) (pointer >>> this.sizeBits);
			final int offset = (int) (pointer & this.sizeMask);
			
			this.currentPageNumber = pageNum;
			
			int posInArray = pageNum - this.segmentNumberOffset;
			seekOutput(this.pages.get(posInArray), offset);
			
			return posInArray;
		}
		
		public void setSegmentNumberOffset(int offset) {
			this.segmentNumberOffset = offset;
		}
	}
	
	
	private static final class ReadView extends AbstractPagedInputView implements SeekableDataInputView {

		private final ArrayList<MemorySegment> segments;
		
		private final int segmentSizeBits;
		
		private final int segmentSizeMask;
		
		private int currentSegmentIndex;
		
		private int segmentNumberOffset;
		
		
		public ReadView(ArrayList<MemorySegment> segments, int segmentSize, int segmentSizeBits) {
			super(segments.get(0), segmentSize, 0);
			
			if ((segmentSize & (segmentSize - 1)) != 0)
				throw new IllegalArgumentException("Segment size must be a power of 2!");
			
			this.segments = segments;
			this.segmentSizeBits = segmentSizeBits;
			this.segmentSizeMask = segmentSize - 1;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
			if (++this.currentSegmentIndex < this.segments.size()) {
				return this.segments.get(this.currentSegmentIndex);
			} else {
				throw new EOFException();
			}
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return this.segmentSizeMask + 1;
		}
		
		@Override
		public void setReadPosition(long position) {
			final int bufferNum = ((int) (position >>> this.segmentSizeBits)) - this.segmentNumberOffset;
			final int offset = (int) (position & this.segmentSizeMask);
			
			this.currentSegmentIndex = bufferNum;
			seekInput(this.segments.get(bufferNum), offset, this.segmentSizeMask + 1);
		}
		
		public void setSegmentNumberOffset(int offset) {
			this.segmentNumberOffset = offset;
		}
	}

}
