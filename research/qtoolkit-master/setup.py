from setuptools import setup, find_packages
import versioneer

setup(
        name='qToolkit',
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),
        description='All-in-One quantitave strategy development toolkit',
        packages=find_packages(),
        package_data={
        # If any package contains *.txt or *.rst files, include them:
        },
        author='Magnum Algo Team',
        url='https://gitlab.aqumon.com/chrisyu/qtoolkit',
        install_requires=[
            'quantcycle @ git+https://gitlab.aqumon.com/algo/quantcycle.git',
            'datamaster @ git+https://gitlab.aqumon.com/pub/py-datamaster-client.git'
        ]
)
