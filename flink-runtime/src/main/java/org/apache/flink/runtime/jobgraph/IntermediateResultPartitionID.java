/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.topology.ResultID;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Id identifying {@link IntermediateResultPartition}.
 */
public class IntermediateResultPartitionID implements Comparable<IntermediateResultPartitionID>, ResultID, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final IntermediateDataSetID intermediateDataSetID;
	private final int partitionNum;

	public IntermediateResultPartitionID() {
		this.partitionNum = -1;
		this.intermediateDataSetID = new IntermediateDataSetID(new AbstractID());
	}

	/**
	 * Creates an new random intermediate result partition ID.
	 */
	public IntermediateResultPartitionID(IntermediateDataSetID intermediateDataSetID, int partitionNum) {
		this.intermediateDataSetID = intermediateDataSetID;
		this.partitionNum = partitionNum;
	}

	public void writeTo(ByteBuf buf) {
		buf.writeLong(intermediateDataSetID.getLowerPart());
		buf.writeLong(intermediateDataSetID.getUpperPart());
		buf.writeInt(partitionNum);
	}

	public static IntermediateResultPartitionID fromByteBuf(ByteBuf buf) {
		long lower = buf.readLong();
		long upper = buf.readLong();
		int partitionNum = buf.readInt();
		IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID(new AbstractID(lower, upper));
		return new IntermediateResultPartitionID(intermediateDataSetID, partitionNum);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			IntermediateResultPartitionID that = (IntermediateResultPartitionID) obj;
			return that.intermediateDataSetID.getLowerPart() == this.intermediateDataSetID.getLowerPart()
				&& that.intermediateDataSetID.getUpperPart() == this.intermediateDataSetID.getUpperPart()
				&& that.partitionNum == this.partitionNum;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return ((int)  this.intermediateDataSetID.getLowerPart()) ^
			((int) (this.intermediateDataSetID.getLowerPart() >>> 32)) ^
			((int)  this.intermediateDataSetID.getUpperPart()) ^
			((int) (this.intermediateDataSetID.getUpperPart() >>> 32)) ^
			(this.partitionNum);
	}

	@Override
	public String toString() {
		return intermediateDataSetID.toString() + "#" + partitionNum;
	}

	@Override
	public int compareTo(IntermediateResultPartitionID o) {
		return 0;
	}
}
