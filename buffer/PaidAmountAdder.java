package com.assignments.assignment5.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class PaidAmountAdder extends BaseOperation implements Buffer {

    public PaidAmountAdder() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();

        TupleEntry destTupleEntry = null;
        int totalPaidAmount = 0;

        while(tupleEntryIterator.hasNext()) {
            TupleEntry tupleEntry = tupleEntryIterator.next();
            totalPaidAmount += tupleEntry.getInteger("paid_amount");
            destTupleEntry = new TupleEntry(tupleEntry);
        }

        destTupleEntry.setInteger("paid_amount", totalPaidAmount);

        bufferCall.getOutputCollector().add(destTupleEntry);
    }
}
