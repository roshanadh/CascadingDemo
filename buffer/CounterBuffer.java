package com.assignments.assignment1.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class CounterBuffer extends BaseOperation implements Buffer {

    public CounterBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        TupleEntry destTupleEntry = new TupleEntry(tupleEntryIterator.next());
        destTupleEntry.setString("count", getIteratorSizeInString(tupleEntryIterator));
        bufferCall.getOutputCollector().add(destTupleEntry);
    }

    private String getIteratorSizeInString(Iterator<TupleEntry> tupleEntryIterator) {
        AtomicLong count = new AtomicLong(1L);
        tupleEntryIterator.forEachRemaining(entry -> count.addAndGet(1));
        return String.valueOf(count);
    }
}
