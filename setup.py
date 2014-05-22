from setuptools import setup

setup(name='HTRC Feature Reader',
      version='0.1',
      description='Library for working with HTRC Feature Extraction dataset',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'License :: OSI Approved :: University of Illinois/NCSA Open Source License',
          'Programming Language :: Python :: 2.7',
          'Natural Language :: English',
      ],
      keywords='hathitrust text-mining text-analysis features',
      url='https://github.com/organisciak/htrc-feature-reader',
      author='Peter Organisciak',
      author_email='organisciak@gmail.com',
      license='NCSA',
      packages=['htrc_features'],
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
