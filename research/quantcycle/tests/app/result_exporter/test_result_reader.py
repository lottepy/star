import os
import numpy as np
from timeit import default_timer as timer
from quantcycle.app.result_exporter.result_reader import ResultReader
from tests.app.result_exporter import test_result_export


def test_get_strategy_1():
    # Test for export in backtesting
    if not os.path.exists(r'results/backtest_results_noflatten.hdf5'):
        test_result_export.test_export_sample_1()
    reader = ResultReader(r'results/backtest_results_noflatten.hdf5')
    results = reader.get_strategy([1, 2], ['position', 'pnl'], df_disable=False)
    print()
    print(results[1][0])  # position
    assert results[1][0].shape == (5, 4)
    print(results[1][1])  # pnl
    assert results[1][1].shape == (5, 1)
    results = reader.get_strategy([1, 2], ['cost'], (1592895501, 1592895518), df_disable=False)
    print(results[2][0])
    assert results[2][0].shape == (2, 4)
    results = reader.get_strategy([], ['cost'], df_disable=False)
    assert not results    # empty dict
    results = reader.get_strategy([1, 2], ['position'], (0, 1), df_disable=False)   # no data in time range
    print(results[1][0])
    assert results[1][0].empty
    reader = ResultReader(r'results/backtest_results_flattened.hdf5')
    results = reader.get_attr([1, 2, 4, 7], ['params', 'symbols'])
    print(results)
    results = reader.get_attr([], ['params', 'symbols'])
    assert results.empty  # empty dataframe
    reader.to_csv(r'results/backtest_results_flattened')
    reader.export_summary(r'results/backtest_results_flattened/summary.csv')

    # Test for export in real trading
    if not os.path.exists(r'results/real_results_noflatten.hdf5'):
        test_result_export.test_save_realtime_data_sample_1()
    reader = ResultReader(r'results/real_results_noflatten.hdf5')
    reader.to_csv(r'results/real_results_noflatten')
    reader.export_summary(r'results/real_results_noflatten/summary.csv')
    reader = ResultReader(r'results/real_results_flattened.hdf5')
    reader.to_csv(r'results/real_results_flattened')
    reader.export_summary(r'results/real_results_flattened/summary.csv', [1, 2, 4, 7, 8, 10, 12, 15])


"""
def test_get_strategy_2(benchmark):
    # Benchmark reading huge file (20GB+) or its compressed version
    start = timer()
    reader = ResultReader(r'E:\Temp\quantcycle\backtest_big_noflatten.hdf5')
    end = timer()
    print(f'\nIndexing took {end - start}s.')
    start = timer()
    results = reader.to_csv(r'E:\Temp\quantcycle', [99, 98])
    end = timer()
    print(f'\nFunc get_strategy() took {end - start}s.')
    results = benchmark(reader.get_strategy, [99, 98], ['position', 'pnl'])
    # print(results)
    all_strategy_ids = reader.get_all_sids()  # views
    print(f'#sids={len(all_strategy_ids)}')
"""


"""
# ----------------------
# Test performance niche
# ----------------------
def test_fake_large_dataset(benchmark):
    import h5py
    faked_3d = np.random.rand(1500, 32, 2086)
    with h5py.File(r'E:\Temp\fake.hdf5', 'w', rdcc_nbytes=3145728, rdcc_w0=1) as hf_out:
        group = hf_out.create_group('faked')
        group.create_dataset('faked_3d', data=faked_3d, compression='gzip', compression_opts=1)

    with h5py.File(r'E:\Temp\fake.hdf5', 'r', rdcc_nbytes=3145728) as hf_in:
        start = timer()
        faked_3d = np.array(hf_in['faked']['faked_3d'])
        end = timer()
        a = faked_3d[98]
        b = faked_3d[199]
        print(f'\nRead faked_3d took {end - start}s.')


def test_get_strategy_3d(benchmark):
    start = timer()
    reader = ResultReader(r'E:\Temp\backtest_big_noflatten.hdf5')
    end = timer()
    print(f'\nIndexing took {end - start}s.')
    id_list = [i for i in range(0, len(reader.get_all_sids()), 2)]
    start = timer()
    results = reader.get_strategy_3d(id_list, 'position')
    end = timer()
    print(f'\nFunc get_strategy_3d() took {end - start}s.')
    id_list = [i for i in range(len(reader.get_all_sids()))]
    results = benchmark(reader.get_strategy_3d, id_list, 'position')
"""
