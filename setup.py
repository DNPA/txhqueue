from setuptools import setup, find_packages
from os import path

setup(
    name='txhqueue',
    version='0.1.1',
    description='Asynchonous hysteresis-queue implementation.',
    long_description="""A simple asynchronous (both twisted and asyncio) Python 
    library for hysteresis queues.""",
    url='https://github.com/DNPA/txhqueue',
    author='Rob M',
    author_email='pdftool@pirod.nl',
    license='LE-BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'Environment :: Other Environment',
    ],
    keywords='async hysteresis queue highwater lowwater',
    packages=find_packages()
)
