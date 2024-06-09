from constant import *

def cn_to_aqumon_ticker(ticker_input_list):
    """
    000001.SH --> CN_10_000001
    :param ticker_input_list:
    :return:
    """
    ticker_output_list = []
    for index, iuid in enumerate(ticker_input_list):
        ticker_output_list.append('CN_10_' + iuid[:6])

    return ticker_output_list


def cn_to_aqumon_fund_ticker(ticker_input_list):
    """
    CH000528 --> CN_20_000528
    :param ticker_input_list:
    :return:
    """
    ticker_output_list = []
    for index, iuid in enumerate(ticker_input_list):
        ticker_output_list.append('CN_20_' + iuid[-6:])

    return ticker_output_list


def xray_huarun_to_name(ticker_input_list):
    """
    transfer xray_huarun underlying iuid to names and class
    :param ticker_input_list:
    :return: list
    """
    class_output_list = []
    ticker_output_list = []
    for index, iuid in enumerate(ticker_input_list):
        class_output_list.append(XRAY_HUARUN_CLASS_MAPPING[iuid])
        ticker_output_list.append(XRAY_HUARUN_NAME_MAPPING[iuid])

    return [class_output_list, ticker_output_list]


def xray_lcm_to_name(ticker_input_list):
    """
    transfer xray_lcm underlying iuid to names and class
    :param ticker_input_list:
    :return: list
    """
    class_output_list = []
    ticker_output_list = []
    for index, iuid in enumerate(ticker_input_list):
        class_output_list.append(XRAY_LCM_CLASS_MAPPING[iuid])
        ticker_output_list.append(XRAY_LCM_NAME_MAPPING[iuid])

    return [class_output_list, ticker_output_list]



def risk_to_basic(ticker_input_list):
    ticker_output_list = []
    for index, risk in enumerate(ticker_input_list):
        ticker_output_list.append(str(risk) + ' Basic')

    return ticker_output_list

def risk_to_weighting(ticker_input_list):
    """
    transfer risk level to str for layout
    :param ticker_input_list:
    :return:
    """
    ticker_output_list = []
    for index, risk in enumerate(ticker_input_list):
        ticker_output_list.append(str(risk) + ' weighting')

    return ticker_output_list


def int_to_str(ticker_input_list):
    """
    transfer int to str for layout
    :param ticker_input_list:
    :return:
    """
    ticker_output_list = []
    for index, iuid in enumerate(ticker_input_list):
        ticker_output_list.append(str(iuid))

    return ticker_output_list
