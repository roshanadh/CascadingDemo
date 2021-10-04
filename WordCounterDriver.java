package com.assignments.assignment1;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.assignments.assignment1.buffer.CounterBuffer;
import com.assignments.assignment1.function.WordSplitterFunction;

import static com.assignments.assignment1.constant.Constants.sinkFilePath;
import static com.assignments.assignment1.constant.Constants.sourceFilePath;

public class WordCounterDriver {
    public static void main(String[] args) {
        Tap sourceTap = new FileTap(
                new TextLine(new Fields("line")),
                sourceFilePath
        );

        Tap sinkTap = new FileTap(
                new TextDelimited(
                        new Fields("word", "count"),
                        true,
                        "|"
                ),
                sinkFilePath,
                SinkMode.REPLACE
        );

        Pipe pipe = new Pipe("wordCountingPipe");
        // add two fields -- word and count -- to all tuple entries
        pipe = new Each(pipe, new Insert(new Fields("word", "count"), "", 1), Fields.ALL);
        // perform word splitting operation on all tuple entries
        pipe = new Each(pipe, new WordSplitterFunction(), Fields.REPLACE);
        pipe = new GroupBy(pipe, new Fields("word"));
        pipe = new Every(pipe, new CounterBuffer(), Fields.REPLACE);
        pipe = new Discard(pipe, new Fields("line"));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);

        flow.complete();
    }
}
