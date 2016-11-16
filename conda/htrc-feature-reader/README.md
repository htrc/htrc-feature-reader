# BUILDING CONDA PACKAGES

Building for `conda` has confused me at times, so it's worth noting how it is done.

Since the HTRC Feature Reader is pure Python, it can easily be converted to versions for all operating systems. However, at least one dependency doesn't exist in conda, pysolr, so it needs to be built and uploaded to the same channel that the main library is uploaded. To increase coverage, both pysolr and htrc-feature-reader should be built for multiple versions of Python.

1. For each library:
	1.1 `mkdir packages` to have a place to put the built packages
	1.2. For each version of Python that you want (i.e. 2.7, 3.5):
		1.2.1 `conda build --python 3.5 .` At the end of the build process, it will provide a location in a `conda-bld` directory where the a tar.bz2 is placed.
		1.2.2 `cd packages`
		1.2.3 `conda convert {built_file_location} -p all` converts the built file to all targets: linux-32, linux-64, win-32, win-64, osx-64.
		1.2.4 `find . - type f | parallel -n1 anaconda --user htrc upload {}` will upload each file to anaconda cloud.
