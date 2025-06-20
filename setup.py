from setuptools import setup, find_packages

setup(
    name='dbbinance-storage',
    version='0.8',
    packages=find_packages(include=['dbbinance', 'dbbinance.*']),
    url='https://github.com/cubecloud/dbbinance-storage',
    license='MIT',
    author='cubecloud',
    author_email='zenroad60@gmail.com',
    description='create pgsql database for BINANCE symbol_pairs'
)
