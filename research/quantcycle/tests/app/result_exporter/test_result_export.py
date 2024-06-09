import h5py
import os
import numpy as np
import time
import pytest
from timeit import default_timer as timer
from quantcycle.app.result_exporter.result_export import ResultExport
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.utils.production_constant import InstrumentType, OrderFeedback


def test_export_sample_1():
    pms, symbols, params = fake_pms_sample_1()
    result_export = ResultExport(r'results')        # saved to folder tests\results
    result_export.export('backtest_results_noflatten.hdf5', (pms, symbols, params), (pms, symbols, params),
                         (pms, symbols, params))
    result_export.export('backtest_results_flattened.hdf5', (pms, symbols, params), (pms, symbols, params),
                         flatten=True, reset_id=0)
    result_export.finish_export('backtest_results_noflatten.hdf5')
    result_export.finish_export('backtest_results_flattened.hdf5')
    __check_hdf5_sample_1(result_export)


def __check_hdf5_sample_1(result_export):
    export_folder = result_export.export_folder
    with h5py.File(os.path.join(export_folder, 'backtest_results_noflatten.hdf5'), 'r') as hf_noflat:
        assert len(hf_noflat) == 7  # fixed
        position = hf_noflat.get('position')
        assert position.shape == (3, 5, 4)  # 3 strategies, 5 timestamps, 4 symbols
        time_mat = hf_noflat.get('time')
        assert time_mat.shape == (3, 5)     # 3 strategies, 5 timestamps
        assert hf_noflat.get('strategy_attr/0').attrs['params'].shape == (2,)
    with h5py.File(os.path.join(export_folder, 'backtest_results_flattened.hdf5'), 'r') as hf_flat:
        assert len(hf_flat) == 7    # fixed
        position = hf_flat.get('position')
        assert position.shape == (8, 5)     # 8 strategies, 5 timestamps
        time_mat = hf_flat.get('time')
        assert time_mat.shape == (8, 5)     # 8 strategies, 5 timestamps
        assert hf_flat.get('strategy_attr/0').attrs['params'].shape == (2,)


def fake_pms_sample_1():
    cash = 1000000
    ccy_matrix = np.array([[1, 1, 0, 1], [0, 0, 1, 0]], dtype=np.float64)
    instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value, InstrumentType.CN_STOCK.value,
                                InstrumentType.US_STOCK.value])
    portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

    msg = {OrderFeedback.transaction.value: np.array([100, 100, 100, 100], dtype=np.float64),
           OrderFeedback.current_data.value: np.array([20, 40, 100, 120], dtype=np.float64),
           OrderFeedback.commission_fee.value: np.array([0, 0, 0, 0], dtype=np.float64),
           OrderFeedback.current_fx_data.value: np.array([2, 4], dtype=np.float64) @ ccy_matrix}
    current_fx_data = msg[OrderFeedback.current_fx_data.value]

    portfolio_manager.receive_order_feedback(msg)
    portfolio_manager.capture(1592445210)
    portfolio_manager.capture(1592446193)
    portfolio_manager.capture(1592451474)
    portfolio_manager.capture(1592895501)
    portfolio_manager.capture(1592895518)
    params = (5, 0.01)
    return portfolio_manager, ['A', 'B', 'C', 'D'], params


"""
def test_export_sample_2():
    # ------------------------------------------
    # Generating big samples for pressure test.
    # NEEDS 30GB SPACE IF COMPRESSION IS NOT ENABLED ON (750, 3000) 100!!!
    # ------------------------------------------
    # (8, 2086) 1500
    # (750, 3000) 100
    start = timer()
    pms, symbols, params = fake_pms_sample_2(8, 2086)
    result_export = ResultExport(r'E:\Temp\quantcycle')
    pms_pack = []
    for i in range(1500):           # num of strategies
        pms_pack.append((pms, symbols, params))
    end = timer()
    print(f'\nGenerating data took {end - start}s.')

    start = timer()
    result_export.export('backtest_big_noflatten.hdf5', *pms_pack)
    end = timer()
    print(f'\nCreating big no-flatten hdf5 took {end - start}s.')
    result_export.finish_export('backtest_big_noflatten.hdf5')

    start = timer()
    result_export.export('backtest_big_flattened.hdf5', *pms_pack, flatten=True, reset_id=0)
    end = timer()
    print(f'\nCreating big flattened hdf5 took {end - start}s.')
    result_export.finish_export('backtest_big_flattened.hdf5')
"""


def fake_pms_sample_2(multiplier, n_timestamps):
    # Populate 4 x multiplier symbols, n_timestamps
    cash = 1000000
    ccy_matrix = np.array([[1, 1, 0, 1] * multiplier, [0, 0, 1, 0] * multiplier], dtype=np.float64)
    instrument_type = np.array([InstrumentType.FX.value, InstrumentType.HK_STOCK.value, InstrumentType.CN_STOCK.value,
                                InstrumentType.US_STOCK.value] * multiplier)
    portfolio_manager = PorfolioManager(cash, ccy_matrix, instrument_type)

    msg = {OrderFeedback.transaction.value: np.array([100, 100, 100, 100] * multiplier, dtype=np.float64),
           OrderFeedback.current_data.value: np.array([20, 40, 100, 120] * multiplier, dtype=np.float64),
           OrderFeedback.commission_fee.value: np.array([0, 0, 0, 0] * multiplier, dtype=np.float64),
           OrderFeedback.current_fx_data.value: np.array([2, 4], dtype=np.float64) @ ccy_matrix}
    current_fx_data = msg[OrderFeedback.current_fx_data.value]

    portfolio_manager.receive_order_feedback(msg)
    current_time = int(time.time())
    for i in range(n_timestamps):
        portfolio_manager.capture(current_time + i)
    params = (5, 0.01)          # fake
    return portfolio_manager, ['A', 'B', 'C', 'D'] * multiplier, params


def test_extract_data_sample_1():
    pms, symbols, params = fake_pms_sample_1()
    result_export = ResultExport(r'results')
    for position, pnl, cost, strategy_id in result_export.extract_data(pms):
        assert len(position) == len(pnl) == len(cost) == 4
        assert strategy_id == 0
    n_symbols = 0
    last_strategy_id = None
    for position, pnl, cost, strategy_id in result_export.extract_data(pms, flatten=True, reset_id=0):
        n_symbols += 1
        last_strategy_id = strategy_id
        assert isinstance(position, float)
    assert n_symbols == 4
    assert last_strategy_id == 3


def test_save_realtime_data_sample_1():
    pms, symbols, params = fake_pms_sample_1()
    result_export = ResultExport(r'results')

    # No-flatten tests
    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1592809718),
                                     (pms, symbols, params, 1592809741), (pms, symbols, params, 1592809763), phase='update_0')
    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1592979967),
                                     (pms, symbols, params, 1592979981), (pms, symbols, params, 1592979990), phase='update_1',
                                     reset_id=0)        # append pms to same IDs

    # Test of different pms pack length when reset_id is enabled
    with pytest.raises(ValueError) as e:
        result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1594952980),
                                         (pms, symbols, params, 1594952989),
                                         phase='update_1', reset_id=0)
    assert "length doesn't match" in str(e)

    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1594953651),
                                     (pms, symbols, params, 1594953658), (pms, symbols, params, 1594953664), phase='update_0',
                                     reset_id=10)
    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1594953810),
                                     (pms, symbols, params, 1594953816), (pms, symbols, params, 1594953823), phase='update_1',
                                     reset_id=10)
    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1594953981),
                                     (pms, symbols, params, 1594953986), (pms, symbols, params, 1594953992),
                                     (pms, symbols, params, 1594953999), phase='update_0',
                                     reset_id=20)
    # It is not recommended to go back to write to the old strategy ID group
    # after starting writing to the new group!
    result_export.save_realtime_data('real_results_noflatten.hdf5', (pms, symbols, params, 1594954045),
                                     (pms, symbols, params, 1594954054), (pms, symbols, params, 1594954060), phase='update_2',
                                     reset_id=10, check_pms_length=False)       # set check_pms_length=False

    # Flattened tests
    # symbols = ['A', 'B', 'C', 'D']
    result_export.save_realtime_data('real_results_flattened.hdf5', (pms, symbols, params, 1592809718),
                                     (pms, symbols, params, 1592809741), phase='update_0', flatten=True, reset_id=0)
    result_export.save_realtime_data('real_results_flattened.hdf5', (pms, symbols, params, 1592980103),
                                     (pms, symbols, params, 1592980111), phase='update_1', flatten=True, reset_id=0)    # append

    result_export.save_realtime_data('real_results_flattened.hdf5', (pms, symbols, params, 1592809763),
                                     phase='update_single_0', flatten=True)      # id should continue
    result_export.save_realtime_data('real_results_flattened.hdf5', (pms, symbols, params, 1594929137),
                                     phase='update_single_0', flatten=True)      # id should continue
    # -----------------------------------------------------------------
    # Note that to explicitly close the current HDF5 is not required.
    #     - A new file name will automatically close the old file.
    # -----------------------------------------------------------------
    result_export.save_realtime_data('#close#')         # explicitly close the current HDF5
    result_export.save_realtime_data('#close#')         # robust against duplicate orders---no effect

    # Extra
    result_export.save_realtime_data('real_results_noflatten_2.hdf5', (pms, symbols, params, 1592809936),
                                     reset_id=0)
    result_export.save_realtime_data('real_results_noflatten_2.hdf5')     # robust---no effect
    result_export.save_realtime_data('#close#')         # explicitly close the current HDF5
    assert True         # you should check the output file manually
