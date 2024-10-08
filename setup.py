from setuptools import setup, find_packages

setup(
    name='dbbinance-storage',
    version='0.78',
    packages=find_packages(include=['dbbinance', 'dbbinance.*']),
    url='https://github.com/cubecloud/dbbinance-storage',
    license='MIT',
    author='cubecloud',
    author_email='zenroad60@gmail.com',
    description='create local database for BINANCE symbol_pairs'
)
