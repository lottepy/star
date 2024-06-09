from setuptools import setup, find_packages
import versioneer

setup(
        name='metrics_calculator',
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        description='Magnum Research Tool:: Metrics Calculator',
        packages=find_packages(include=['metrics_calculator', 'metrics_calculator.*']),
        package_data={
            "metrics_calculator": ["data/*"]
        },
        # package_data={
        # # If any package contains *.txt or *.rst files, include them:
        # '': ['*.csv', '*.txt', '*.md','*.xls', '*.ini', '*.json','*.xlsx']
        # },
        author='Magnum Algo Team',
        url='https://https://gitlab.aqumon.com/junxinzhou/metrics_calculator',
        install_requires=[
                          'backtest-engine @ git+ssh://git@gitlab.aqumon.com/algo/HFT-bt-framework.git',
                          'datamaster @ git+ssh://git@gitlab.aqumon.com/pub/py-datamaster-client.git',
                          'metrics_calculator_core @ git+ssh://git@gitlab.aqumon.com/pub/metrics-calculator-core.git',
                          'numpy==1.17.4',
                          'pandas==0.25.3',
                          'numba==0.47',
                          'h5py==2.10.0'
                          ]
)
