import pytest
from quantcycle.app.result_exporter.utils import OrderLogger


def test_order_logger():
    logger = OrderLogger('results')
    log_dict_list = __create_samples()
    logger.dump_checker_log('order_checker_logger.log', log_dict_list)
    __append_samples(log_dict_list)
    logger.dump_checker_log('order_checker_logger.log', log_dict_list)
    logger.flush_checker_log()
    __append_wrong(log_dict_list)
    with pytest.raises(ValueError) as e:
        logger.dump_checker_log('order_checker_logger.log', log_dict_list)

    log_dict_list = __create_samples()
    logger.dump_checker_log('order_checker_logger_2.log', log_dict_list)
    __append_samples(log_dict_list)
    logger.dump_checker_log('order_checker_logger_2.log', log_dict_list)


def __create_samples():
    log_dict_list = [{1592445210: 'This is the beginning of 0.',
                      1592446193: 'Hah~~',
                      1592451474: 'Hello'},
                     {1592445210: 'This is the beginning of 1.',
                      1592451474: 'Hello'},
                     {1592445210: 'This is the beginning of 2.',
                      1592446193: 'Hah~~',
                      1592451474: 'Hello'},
                     {1592445210: 'This is the beginning of 3.',
                      1592451474: 'Hello, hello...'},
                     {1592445210: 'This is the beginning of 4.',
                      1592446193: 'Hah~~',
                      1592451474: 'Hello'}]
    return log_dict_list


def __append_samples(log_dict_list):
    log_dict_list[0].update({1592895501: 'One more thing...'})
    log_dict_list[1].update({})
    log_dict_list[2].update({1592895501: 'One more thing...'})
    log_dict_list[3].update({})
    log_dict_list[4].update({1592895501: 'Two more things...',
                             1592895518: 'End of game!'})


def __append_wrong(log_dict_list):
    log_dict_list.append({1592895501: 'One more thing...'})
