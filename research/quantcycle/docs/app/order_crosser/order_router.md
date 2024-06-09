# Order Router

## Introduction

This module aims at exchange the order to receive the order feedback.
There are two routers in this module. One is `OrderRouter` which is a mock class to get the order feedback. The other one is `FTP_Order_Route`, which can send order to the real server and receive the feedback.
`FTP_Order_Route` can only be used by live trading.


### cross_order Function

This is the most important function in this class.

Every time we input a order array, it will return a feedback message array `matrix_msg`. It would be put in the queue for futher use.

Format of `matrix_msg` is a 2D-array (6,symbols)



Example below is the cross_order Function in `OrderRouter`, which used for backtesting.
```

        matrix_msg[OrderFeedback.transaction.value] = np.array(order)
        matrix_msg[OrderFeedback.current_data.value] = self.execution_current_data[symbol_index]
        matrix_msg[OrderFeedback.current_fx_data.value] = self.execution_current_fx_data[symbol_index]
        if self.commission_pool_path != None:
            com=self.commission.loc[symbols].values.flatten()
            matrix_msg[OrderFeedback.commission_fee.value] = np.abs(order) * self.execution_current_data[symbol_index] * \
                                                             self.execution_current_fx_data[symbol_index] \
                                                             * com[symbol_index]
        else:

            matrix_msg[OrderFeedback.commission_fee.value] = np.abs(order) * self.execution_current_data[symbol_index] * self.execution_current_fx_data[symbol_index] \
                                                                * self.commission_fee

        matrix_msg[OrderFeedback.order_status.value] = np.array([OrderStatus.FILLED.value for i in range(len(order))])
        matrix_msg[OrderFeedback.timestamps.value] = np.array([timestamps[0][0] for i in range(len(order))])
        task_type = "order_feedback"
        self.order_feedback_queue.put((strategy_id,task_type,matrix_msg,ts))

```






