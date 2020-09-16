from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setup(name='htrc-feature-reader',
      version='2.0.7',
      description='Library for working with the HTRC Extracted Features dataset',
      long_description=long_description,
      long_description_content_type="text/markdown",
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
      install_requires=['six', 'pandas>=0.24', 'requests', 'python-rapidjson', 'pymarc'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      entry_points={
          'console_scripts': [
              'htid2rsync = htrc_features.utils:htid2rsync_cmd'
              ]
          },
      zip_safe=False)
