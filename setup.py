#!/usr/bin/env python
"""
Bucket Limiter
----------------

Example
```````

.. code:: python

    TODO:

Links
`````

* `Github <https://github.com/TheWaWaR/bucket-limiter-py>`

"""

from setuptools import setup

setup(
    name='Bucket-Limiter',
    version='0.2.0',
    url='https://github.com/TheWaWaR/bucket-limiter-py',
    license='MIT',
    author='Qian Linfeng',
    author_email='thewawar@gmail.com',
    description='Token bucket like limiter library based on redis storage',
    long_description=__doc__,
    packages=['bucket_limiter'],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'redis>=2.10.5'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ]
)
