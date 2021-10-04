package com.assignments.assignment4;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.assignments.assignment4.buffer.NameBuffer;
import com.assignments.assignment4.filter.SSNFilter;

import static com.assignments.assignment4.constant.Constants.sinkFilePath;
import static com.assignments.assignment4.constant.Constants.sourceFilePath;

public class NameBufferDriver {
    public static void main(String[] args) {
        Tap sourceTap = new FileTap(
                new TextDelimited(
                        true,
                        ";"
                ),
                sourceFilePath
        );

        Tap sinkTap = new FileTap(
                new TextDelimited(
                        true,
                        "|"
                ),
                sinkFilePath,
                SinkMode.REPLACE
        );

        Pipe pipe = new Pipe("nameBufferPipe");

        // Filter out records with ssn != 9 digits
        //pipe = new Each(pipe, new SSNFilter());
        pipe = new Each(pipe, new Fields("ssn"), new ExpressionFilter("String.valueOf(ssn).length()!=9", String.class));

        Fields groupingField = new Fields("ssn");
        Fields sortingField = new Fields("name");

        pipe = new GroupBy(pipe, groupingField, sortingField, false);

        pipe = new Every(pipe, new NameBuffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);

        flow.complete();
    }
}
