from setuptools import setup
import sys
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""

requires = [
    'simplejson',
    'pyzmq',
    'aiozmq',
    'aiohttp~=2.2.0',
    'aiodns',
    'async_timeout',
]
build_requires = [
    'pypandoc',
    'wheel',
    'twine',
]
test_requires = [
    'pytest>=3.1',
    'pytest-cov',
    'pytest-asyncio',
    'pytest-mock',
    'asynctest',
    'codecov',
    'flake8',
]
dev_requires = build_requires + test_requires + [
    'pytest-sugar',
]
ci_requires = []
monitor_requires = [
    'datadog>=5.2',
    'raven>=6.1',
]


setup(
    name='sorna-common',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.9.5',
    description='Sorna common libraries',
    long_description=long_description,
    url='https://github.com/lablup/sorna-common',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],

    packages=['sorna'],
    namespace_packages=['sorna'],

    python_requires='>=3.6',
    install_requires=requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'ci': ci_requires,
        'monitor': monitor_requires,
    },
)
