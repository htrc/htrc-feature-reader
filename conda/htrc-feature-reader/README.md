# BUILDING CONDA PACKAGES

Building for `conda` has confused me at times, so it's worth noting how it is done.

Since the HTRC Feature Reader is pure Python, it can easily be converted to versions for all operating systems. However, at least one dependency doesn't exist in conda, pysolr, so it needs to be built and uploaded to the same channel that the main library is uploaded. To increase coverage, both pysolr and htrc-feature-reader should be built for multiple versions of Python.

1. For each library:
	1.1. `mkdir packages` to have a place to put the built packages
	1.2. Update version number in meta.yaml if necessary.
    1.3 `conda build .`
	1.4 `cd packages`
	1.5 `conda convert {built_file_location} -p all` converts the built file to all targets: linux-32, linux-64, win-32, win-64, osx-64.
	1.6 `find . -type f | parallel -n1 anaconda upload --user htrc {}` will upload each file to anaconda cloud.
	1.7 `conda build purge`

## Quick command for build and upload

```
mkdir packages
conda build .
cd packages
ls ~/anaconda3/conda-bld/linux-64/htrc-feature-reader* | parallel conda convert {} -p all
find . -type f | parallel -n1 anaconda upload --user htrc {}
cd .. && rm -rf packages && conda build purge
```
