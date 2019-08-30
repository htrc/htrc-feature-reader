# BUILDING CONDA PACKAGES

Building for `conda` has confused me at times, so it's worth noting how it is done.

Since the HTRC Feature Reader is pure Python, it can easily be converted to versions for all operating systems. However, at least one dependency doesn't exist in conda, pysolr, so it needs to be built and uploaded to the same channel that the main library is uploaded. To increase coverage, both pysolr and htrc-feature-reader should be built for multiple versions of Python.

1. For each library:
	1.1. Update version number in meta.yaml if necessary.
    1.2 `conda build .`
	1.3 `anaconda upload --user htrc {built_file_location}` will upload each file to anaconda cloud.
	1.4 `conda build purge`

## Quick command for build and upload

```
conda build .
parallel -n1 anaconda upload --user htrc ~/anaconda3/conda-bld/noarch/htrc-feature-reader*
cd .. && rm -rf packages && conda build purge
```
