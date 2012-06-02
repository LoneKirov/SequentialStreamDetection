from distutils.core import setup

setup(
    name='SequentialStreamDetection',
    version='1.0',
    author='Adam Miller',
    author_email='kirov.sama@gmail.com',
    packages=['sequential_stream'],
    scripts=['bin/streams.py'],
    license='LICENSE',
    description='Python implementation of Sequential Stream Detection',
    long_description=open('README.rst').read()
)
