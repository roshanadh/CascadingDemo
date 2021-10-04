package com.assignments.assignment4.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import javax.swing.text.html.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class NameBuffer extends BaseOperation implements Buffer {

    public NameBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();

        //TupleEntry destTupleEntry = new TupleEntry(tupleEntryIterator.next());

        //String longestName = destTupleEntry.getString("name");
        //int maxLength = longestName.length();
        //
        //while(tupleEntryIterator.hasNext()) {
        //    String name = tupleEntryIterator.next().getString("name");
        //    if (name.length() > maxLength) {
        //        maxLength = name.length();
        //        longestName = name;
        //    }
        //}
        //
        //destTupleEntry.setString("name", longestName);

        bufferCall.getOutputCollector().add(tupleEntryIterator.next());
    }
}
