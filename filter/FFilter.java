package com.assignments.assignment2.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * Filter out non-Female Tuple entries
 */
public class FFilter extends BaseOperation implements Filter {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        return tupleEntry.getString("gender").toLowerCase().equals("m");
    }
}
