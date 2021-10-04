package com.assignments.assignment5;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.assignments.assignment5.buffer.PaidAmountAdder;
import com.assignments.assignment5.comparator.PaidAmountComparator;

import static com.assignments.assignment5.constant.Constants.sinkFilePath;
import static com.assignments.assignment5.constant.Constants.sourceFilePath;

public class PaidAmountAdderDriver {
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

        Pipe pipe = new Pipe("paidAmountAddingPipe");

        Fields groupingFields = new Fields("mbr_id");
        Fields sortingFields = new Fields("paid_amount");

        sortingFields.setComparator("paid_amount", new PaidAmountComparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return Integer.valueOf((String) o1).compareTo(Integer.valueOf((String) o2));
            }
        });

        pipe = new GroupBy(pipe, groupingFields, sortingFields, false);

        pipe = new Every(pipe, new PaidAmountAdder(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);

        flow.complete();
    }
}
