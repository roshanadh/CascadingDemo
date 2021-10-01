package com.genderfilterdemo.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

import java.util.Locale;

// 2. Define GenderFilter class
public class GenderFilter extends BaseOperation implements Filter {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        String gender = tupleEntry.getString("gender");

        return gender.toLowerCase().equals("m") || gender.toLowerCase().equals("f") ? false : true;
    }
}
// 3. Create a pipe on which the above operation is perfomed
