from setuptools import setup

setup(name='htrc-feature-reader',
      version='2.02',
      description='Library for working with the HTRC Extracted Features dataset',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: University of Illinois/NCSA Open Source License',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Natural Language :: English',
      ],
      keywords='hathitrust text-mining text-analysis features',
      url='https://github.com/organisciak/htrc-feature-reader',
      author='Peter Organisciak',
      author_email='organisciak@gmail.com',
      license='NCSA',
      packages=['htrc_features'],
      install_requires=['six', 'pandas>=0.24', 'numpy', 'requests', 'rapidjson', 'pymarc'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      entry_points={
          'console_scripts': [
              'htid2rsync = htrc_features.utils:htid2rsync_cmd'
              ]
          },
      zip_safe=False)
