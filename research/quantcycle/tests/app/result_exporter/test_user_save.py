from quantcycle.app.result_exporter.utils import UserSave


def test_save_signal_remark():
    # For backtest engine's call
    user_save = UserSave('results')
    user_save.save_signal_remark('faked_remark.csv', __fake_a_remark())
    assert True     # check the output file manually


def test_update_signal_remark():
    # For production engine's call in live trading
    user_save = UserSave('results')
    user_save.open_signal_remark('faked_live_remark.csv', 0)
    for k, v in __fake_a_remark().items():
        remark = {k: v}
        user_save.update_signal_remark(remark)
    user_save.close_signal_remark()
    assert True     # check the output file manually


def __fake_a_remark():
    remark = {1599213759: {'a': 1,
                           'ab': 0.1,
                           'abc': 0.01
                           },
              1599213857: {'a': 2,
                           'ab': 0.2,
                           'abc': 0.02
                           },
              1599213895: {'t': 486.31,
                           'a': 3,
                           'aa': 99
                           },
              1599213961: {'a': 4,
                           'ab': 0.4,
                           'abc': 0.04,
                           'c': -273.15
                           },
              1601891123: {'abc': 0.05,
                           'ab': 0.5,
                           'a': 5
                           },
              1601896447: {'a': 6,
                           'abc': 0.06,
                           'c': -273.15
                           }
              }
    return remark
