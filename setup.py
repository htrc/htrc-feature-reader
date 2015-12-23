from setuptools import setup

setup(name='HTRC Feature Reader',
      version='1.3',
      description='Library for working with the HTRC Extracted Features dataset',
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: University of Illinois/NCSA Open Source License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.0',
          'Programming Language :: Python :: 3.2',
          'Natural Language :: English',
      ],
      keywords='hathitrust text-mining text-analysis features',
      url='https://github.com/organisciak/htrc-feature-reader',
      author='Peter Organisciak',
      author_email='organisciak@gmail.com',
      license='NCSA',
      packages=['htrc_features'],
      install_requires=['six', 'pandas', 'numpy'],
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
