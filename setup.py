from setuptools import setup, find_packages

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name = 'flumehandler',
    version = '0.1',
    packages = ['flumehandler'],
    license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description = 'flume handler for logging',
    author = 'yu000hong',
    author_email = 'yu000hong@sina.com',
    url = 'https://github.com/yu000hong/flumehandler',
    download_url = 'https://github.com/yu000hong/flumehandler/archive/v0.1-alpha.tar.gz',
    keywords = ['Flume', 'logging', 'FlumeHandler', 'Handler', 'FlumeAgent'],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: System :: Logging',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
)