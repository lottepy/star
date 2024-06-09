from setuptools import setup, find_packages
import versioneer

setup(
        name='quantcycle',
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        description='quantcycle v1',
        packages=find_packages(include=['quantcycle', 'quantcycle.*']),
        package_data={
        # If any package contains *.txt or *.rst files, include them:
        'quantcycle': ['app/data_manager/data_loader/data_center/data/*',
                       'app/data_manager/logs/.keep',
                       'app/data_manager/data_processor/method/calculate_interest_rate/*']
        
        },
        author='Magnum Algo Team',
        url='https://gitlab.aqumon.com/algo/quantcycle',
        install_requires=[
                'metrics-calculator-core @ git+https://gitlab.aqumon.com/pub/metrics-calculator-core.git',
                'datamaster @ git+https://gitlab.aqumon.com/pub/py-datamaster-client.git',
                'pandas==1.1.3',
                'pytest==5.4.3',
                'matplotlib==3.2.2',
                'numba==0.50.1',
                'h5py==2.10.0',
                'xlrd==1.2.0',
                'PyYAML==5.3.1',
        ]
)
