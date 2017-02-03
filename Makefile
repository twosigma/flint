SHELL = /bin/bash
VERSION = $(shell cat version.txt)

.PHONY: clean clean-pyc clean-dist dist

clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf python/build/
	rm -rf python/*.egg-info

dist: clean-pyc
	sbt "set test in assembly := {}" clean assembly
	cd python; \
		find . -mindepth 2 -name '*.py' -print | \
	    zip ../target/scala-2.11/flint-assembly-$(VERSION)-SNAPSHOT.jar -@
