package com.genderfilterdemo;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import com.genderfilterdemo.filter.GenderFilter;

import static com.genderfilterdemo.util.Constants.genderFilteredSinkFilePath;
import static com.genderfilterdemo.util.Constants.sourceFilePath;

public class GenderFilterDriver {
    public static void main(String[] args) {
        // 1. Define source and sink Taps
        Tap sourceTap = new FileTap(
                new TextDelimited(true, ";"),
                sourceFilePath
        );

        Tap sinkTap = new FileTap(
                new TextDelimited(true, "|"),
                genderFilteredSinkFilePath,
                SinkMode.REPLACE
        );

        // 2. Define GenderFile class
        // 3. Create a pipe on which the GenderFilter operation is performed

        Pipe pipe = new Pipe("genderFilterPipe");
        pipe = new Each(pipe, new GenderFilter());
        // Each pipe will take the previous Pipe instance to perform the GenderFilter operation

        // 4. Create a Flow and execute operation
        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);

        flow.complete();
    }
}
