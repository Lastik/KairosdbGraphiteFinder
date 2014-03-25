# coding: utf-8
from setuptools import setup

setup(
    name='kairosdb',
    version='0.0.1',
    license='BSD',
    author=u'Dmitry Gryzunov',
    description=('A plugin for using graphite-web with the cassandra-based '
                 'Kairosdb storage backend'),
    py_modules=('kairosdb',),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: System :: Monitoring',
    ),
    install_requires=(
        'requests',
    ),
)
