"""Simple-Salesforce Package Setup"""

import os
import sys
import textwrap

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

package_name = 'simple_salesforce'

about = {}
with open(os.path.join(here, 'simple_salesforce', '__version__.py'), 'r') as f:
    exec(f.read(), about)


packages = [
    package
    for package in find_packages(where=".", exclude=("test_elasticsearch*",))
    if package == package_name or package.startswith(package_name + ".")
]

setup(
    name=about['__title__'],
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    maintainer=about['__maintainer__'],
    maintainer_email=about['__maintainer_email__'],
    url=about['__url__'],
    license=about['__license__'],
    description=about['__description__'],
    long_description=textwrap.dedent(open('README.rst', 'r').read()),
    long_description_content_type='text/x-rst',
    packages=packages,
    package_data={"simple_salesforce": ["*.py"]},
    install_requires=[
        'requests>=2.22.0',
        'authlib',
        'zeep',
        'aiohttp',
        'xmltodict>=0.12.0'
    ],
    tests_require=[
        'nose>=1.3.0',
        'pytz>=2014.1.1',
        'responses>=0.5.1',
    ],
    test_suite='nose.collector',
    keywords=about['__keywords__'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: PyPy'
    ]
)
