SHELL = /bin/bash
VERSION = $(shell cat version.txt)
SCALA_TARGET_VERSION=2.12
CASSANDRA_VERSION ?= 4.1.7
PYTHON=python3.11
PIP=pip3
all:;: '$(CASSANDRA_VERSION)'

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
	rm -rf python/dist/
	rm -rf python/*.egg-info
	rm -rf python/pyspark_cassandra/jars/*.jar
	rm -f python/README.md
	rm -rf .tox
	rm -rf .ccm
	rm -rf venv

install-venv:
	test -d venv || $(PYTHON) -m venv venv
	venv/bin/$(PIP) install --upgrade pip
	venv/bin/$(PIP) install -r python/requirements.txt
	venv/bin/$(PIP) install -r python/requirements_dev.txt
	venv/bin/$(PIP) install --no-deps git+https://github.com/riptano/ccm.git@7efd4c6cabd85499e53a475be81e028d59bcb15b
	venv/bin/$(PIP) install -e python/

reinstall-venv: clean-dist install-venv

start-cassandra: install-venv
	mkdir -p .ccm
	venv/bin/ccm status || venv/bin/ccm create pyspark_cassandra_test -v $(CASSANDRA_VERSION) -n 1
	@# Cassandra 4.x ships JVM options incompatible with Java 17; use the jdk17-specific file
	@for node_dir in $$HOME/.ccm/pyspark_cassandra_test/node*; do \
		if [ -f "$$node_dir/conf/jvm17-server.options" ]; then \
			cp "$$node_dir/conf/jvm17-server.options" "$$node_dir/conf/jvm-server.options"; \
		fi; \
	done
	venv/bin/ccm start
	
stop-cassandra:
	venv/bin/ccm remove

test: test-python test-scala test-integration

test-python:

test-scala:

test-integration: dist test-integration-setup test-integration-matrix test-integration-teardown
	
test-integration-setup: start-cassandra

test-integration-teardown: stop-cassandra
	
test-integration-matrix: test-integration-spark-3.5.5

test-integration-spark-3.5.5:
	$(call test-integration-for-version,3.5.5,hadoop3)

define test-integration-for-version
	echo ======================================================================
	echo testing integration with spark-$1
	
	mkdir -p lib && test -d lib/spark-$1-bin-$2 || \
		(pushd lib && curl https://archive.apache.org/dist/spark/spark-$1/spark-$1-bin-$2.tgz | tar xz && popd)

	test -f lib/spark-$1-bin-$2/conf/log4j2.properties || \
		cp log4j2.properties lib/spark-$1-bin-$2/conf/ 2>/dev/null || true
	
	cp log4j.properties lib/spark-$1-bin-$2/conf/ 2>/dev/null || true

	source venv/bin/activate ; \
		lib/spark-$1-bin-$2/bin/spark-submit \
			--master local[*] \
			--driver-memory 512m \
			--conf spark.cassandra.connection.host="localhost" \
			--jars $$(python -c "import pyspark_cassandra; print(pyspark_cassandra.jar_path())") \
			python/tests.py
			
	echo ======================================================================
endef

dist: clean-pyc
	sbt -batch assembly
	cp target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra-assembly-$(VERSION).jar \
		python/pyspark_cassandra/jars/
	cp README.md python/README.md

all: clean lint dist

publish: dist install-venv
	cd python && ../venv/bin/python -m build
	cd python && ../venv/bin/twine upload dist/* -r prod

lint: python-tox scala-style

python-tox: ## check style with flake8
	tox

scala-style: ## check style with scalastyle
	sbt -batch scalastyle

release-staging: clean dist install-venv ## package and upload a release to staging PyPi
	cd python && ../venv/bin/python -m build
	cd python && ../venv/bin/twine upload dist/* -r staging

release-prod: clean dist install-venv ## package and upload a release to prod PyPi
	cd python && ../venv/bin/python -m build
	cd python && ../venv/bin/twine upload dist/* -r prod
