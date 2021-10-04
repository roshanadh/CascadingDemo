package com.assignments.assignment4.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class SSNFilter extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        //System.out.println(String.valueOf(tupleEntry.getLong("ssn")).length() == 9);
        return String.valueOf(tupleEntry.getLong("ssn")).length() != 9;
    }
}
