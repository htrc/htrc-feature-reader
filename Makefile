all cleancheck:
	python setup.py sdist
	twine upload dist/*

cleancheck:
	@status=$$(git status --porcelain); \
	if test "x$${status}" = x; then \
		git branch -f deployment; \
		git push origin deployment; \
	else \
		echo Working directory is dirty >&2; \
		exit 1
	fi
