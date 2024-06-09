from timeit import default_timer as timer
from multiprocessing import Process
from tests.app.result_exporter.test_result_export import fake_pms_sample_2
from quantcycle.app.result_exporter.result_export import ResultExport


"""
# -------------------------------------------------
# Test functions replaced by test_export_parallel()
# -------------------------------------------------
def test_export():
    # Prepare data
    #    - 1500 strategies, 32 symbols, 8 years daily (2086 days)
    pms, symbols, params = fake_pms_sample_2(8, 2086)
    pms_pack = []
    for i in range(1500):  # num of strategies
        pms_pack.append((pms, symbols, params))

    # Test of non-flatten
    batch_size = 10
    pms_slice = [pms_pack[bid * batch_size:(bid + 1) * batch_size] for bid in range(10)]
    start = timer()
    jobs = []
    for bid in range(10):  # num of processes
        p = Process(target=__export_worker, args=(bid, pms_slice[bid], bid * batch_size, False))
        jobs.append(p)
        p.start()
    for job in jobs:
        job.join()  # wait until all jobs to finish
    end = timer()
    print(f'\nCreating big no-flatten hdf5 took {end - start}s.')

    # Test of flatten
    batch_size = 10
    pms_slice = [pms_pack[bid * batch_size:(bid + 1) * batch_size] for bid in range(10)]
    n_symbols = len(pms_pack[0][1])
    start = timer()
    jobs = []
    for bid in range(10):  # num of processes
        p = Process(target=__export_worker, args=(bid, pms_slice[bid], bid * batch_size * n_symbols, True))
        jobs.append(p)
        p.start()
    for job in jobs:
        job.join()
    end = timer()
    print(f'\nCreating big flattened hdf5 took {end - start}s.')


def __export_worker(worker_id, pms_pack, id_start, flatten=False):
    result_export = ResultExport(r'results/parallel_export_test')
    if not flatten:
        result_export.export('backtest_big_noflatten.hdf5', *pms_pack, flatten=flatten, reset_id=id_start,
                             process_id=worker_id)
        result_export.finish_export('backtest_big_noflatten.hdf5', process_id=worker_id)
    else:
        result_export.export('backtest_big_flattened.hdf5', *pms_pack, flatten=flatten, reset_id=id_start,
                             process_id=worker_id)
        result_export.finish_export('backtest_big_flattened.hdf5', process_id=worker_id)
"""


def test_export_parallel():
    # Prepare data
    #    - 1500 strategies, 32 symbols, 8 years daily (2086 days)
    pms, symbols, params = fake_pms_sample_2(8, 2086)
    pms_pack = []
    for i in range(1500):  # num of strategies
        pms_pack.append((pms, symbols, params))

    # Test of non-flatten
    result_export = ResultExport(r'results/parallel_export_test')
    start = timer()
    result_export.export_parallel('backtest_big_noflatten.hdf5', 5, pms_pack, flatten=False)
    end = timer()
    print(f'\nCreating big no-flatten hdf5 took {end - start}s.')

    # Test of flatten
    result_export = ResultExport(r'results/parallel_export_test')
    start = timer()
    result_export.export_parallel('backtest_big_flattened.hdf5', 5, pms_pack, flatten=True)
    end = timer()
    print(f'\nCreating big flattened hdf5 took {end - start}s.')
