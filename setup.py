#!/usr/bin/python

from setuptools import setup

setup(name='izaber_plpython_zerp',
      version='1.1',
      description='Base load point for iZaber plpython for Zerp',
      url = 'https://github.com/zabertech/izaber-plpython-zerp',
      download_url = 'https://github.com/zabertech/python-plpython-zerp/archive/1.1.tar.gz',
      author='Aki Mimoto',
      author_email='aki+izaber@zaber.com',
      license='MIT',
      packages=['izaber_plpython_zerp'],
      scripts=[],
      install_requires=[
          'izaber',
          'izaber-plpython',
      ],
      dependency_links=[],
      zip_safe=False)

