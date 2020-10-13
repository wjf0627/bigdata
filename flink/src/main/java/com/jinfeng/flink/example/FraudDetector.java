package com.jinfeng.flink.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * @package: com.jinfeng.flink.example
 * @author: wangjf
 * @date: 2020/8/27
 * @time: 5:41 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 5355061523679350650L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);

        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);

        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {

        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long timer = timerState.value();

        ctx.timerService().deleteProcessingTimeTimer(timer);

        timerState.clear();
        flagState.clear();
    }
}
