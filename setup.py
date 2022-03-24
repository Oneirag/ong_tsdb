from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='ong_tsdb',
    version='0.6.4',
    packages=['ong_tsdb'],
    url='www.neirapinuela.es',
    license='',
    author='Oscar Neira Garcia',
    author_email='oneirag@yahoo.es',
    description='Simple Time Series DataBase, based on plain files and fixed interval data',
    install_requires=required,
    entry_points={
        'console_scripts': [
            'ong_tsdb_server = ong_tsdb.server:main',
        ],
    }
)
