from setuptools import setup

setup(name='htrc-feature-reader',
      version='1.94',
      description='Library for working with the HTRC Extracted Features dataset',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: University of Illinois/NCSA Open Source License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.2',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Natural Language :: English',
      ],
      keywords='hathitrust text-mining text-analysis features',
      url='https://github.com/organisciak/htrc-feature-reader',
      author='Peter Organisciak',
      author_email='organisciak@gmail.com',
      license='NCSA',
      packages=['htrc_features'],
      install_requires=['six', 'pandas', 'numpy', 'pysolr', 'bz2file',
                        'ujson'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      entry_points={
          'console_scripts': [
              'htid2rsync = htrc_features.utils:htid2rsync_cmd'
              ]
          },
      zip_safe=False)
