package com.magnumresearch.aqumon.riskengine.adapter.ayers.constants;

public class MessageConstants {
    public static final String MESSAGE_TYPE_LOGIN_REQUEST = "login";
    public static final String MESSAGE_TYPE_KEEP_ALIVE = "order_action:keep_alive";
    public static final String MESSAGE_TYPE_ADD_ORDER = "order_action:Add";
    public static final String MESSAGE_TYPE_UPDATE_ORDER = "order_action:Update";
    public static final String MESSAGE_TYPE_CANCEL_ORDER = "order_action:Cancel";
    public static final String MESSAGE_TYPE_CASH_IN_NOTICE = "cash_in";
    public static final String MESSAGE_TYPE_CASH_OUT_REQUEST = "cash_out";
    public static final String MESSAGE_TYPE_ORDER_REQUEST = "order_enq";


    //Response for login, action
    public static final String MESSAGE_TYPE_RESPONSE = "response";
    public static final String MESSAGE_TYPE_ORDER_NOTIFICATION= "order_notification";
    public static final String MESSAGE_TYPE_TRADE_NOTIFICATION = "trade_notification";
    public static final String MESSAGE_TYPE_ORDER_RECOVERY = "order_recovery";

    //Response for
    public static final String MESSAGE_TYPE_PORTFOLIO_REQUEST = "portfolio";
    public static final String MESSAGE_TYPE_BALANCE_REQUEST = "client_balance";
    public static final String MESSAGE_TYPE_PORTFOLIO_RESPONSE = "portfolio_response";
    public static final String MESSAGE_TYPE_ORDER_RESPONSE = "order_enq_response";

    public static final String MESSAGE_TYPE_CLIENT_ORDER_REQUEST = "client_order";
    public static final String MESSAGE_TYPE_CASH_IO_NOTIFICATION = "cash_io_notification";
    public static final String MESSAGE_TYPE_BALANCE_RESPONSE = "client_balance_response";
}
