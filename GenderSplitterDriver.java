package com.assignments.assignment2;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import com.assignments.assignment2.filter.FFilter;
import com.assignments.assignment2.filter.MFilter;

import static com.assignments.assignment2.constant.Constants.*;

public class GenderSplitterDriver {
    public static void main(String[] args) {
        Tap sourceTap = new FileTap(
                new TextDelimited(
                        true,
                        ";"
                ),
                sourceFilePath
        );

        Tap sinkMTap = new FileTap(
                new TextDelimited(
                        true,
                        "|"
                ),
                sinkMFilePath,
                SinkMode.REPLACE
        );

        Tap sinkFTap = new FileTap(
                new TextDelimited(
                        true,
                        "|"
                ),
                sinkFFilePath,
                SinkMode.REPLACE
        );

        Pipe initialPipe = new Pipe("genderFilterPipe");
        // Pipe to filter out non-male entries
        Pipe toMPipe = new Pipe("toMPipe", initialPipe);
        toMPipe = new Each(toMPipe, new MFilter());

        // Pipe to filter out non-female entries
        Pipe toFPipe = new Pipe("toFPipe", initialPipe);
        toFPipe = new Each(toFPipe, new FFilter());

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(initialPipe, sourceTap)
                .addTailSink(toMPipe, sinkMTap)
                .addTailSink(toFPipe, sinkFTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
