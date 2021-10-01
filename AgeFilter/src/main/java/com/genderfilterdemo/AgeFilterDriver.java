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
import com.genderfilterdemo.filter.AgeFilter;


import static com.genderfilterdemo.util.Constants.ageFilteredSinkFilePath;
import static com.genderfilterdemo.util.Constants.sourceFilePath;

public class AgeFilterDriver {
    public static void main(String[] args) {
        Tap sourceTap = new FileTap(
                new TextDelimited(true, ";"),
                sourceFilePath
        );

        Tap sinkTap = new FileTap(
                new TextDelimited(true, "|"),
                ageFilteredSinkFilePath,
                SinkMode.REPLACE
        );

        Pipe pipe = new Pipe("ageFilterPipe");
        pipe = new Each(pipe, new AgeFilter());

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);

        flow.complete();
    }
}
