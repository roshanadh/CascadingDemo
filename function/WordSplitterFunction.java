package com.assignments.assignment1.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Splits input line into words
 */
public class WordSplitterFunction extends BaseOperation implements Function {

    public WordSplitterFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry sourceTupleEntry = functionCall.getArguments();
        TupleEntry destTupleEntry = new TupleEntry(sourceTupleEntry);

        String line = sourceTupleEntry.getString("line");
        String[] words = line.split("\\s+");

        for (String word: words) {
            destTupleEntry.setString("word", word);
            functionCall.getOutputCollector().add(destTupleEntry);
        }
    }
}
