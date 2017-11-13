SHELL = /bin/bash
VERSION = $(shell cat version.txt)
SCALA_TARGET_VERSION=2.11

.PHONY: clean clean-pyc clean-dist dist test-travis

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
	rm -rf .tox
	rm -rf .ccm
	rm -rf venv

install-venv:
	test -d venv || virtualenv venv
	
install-cassandra-driver: install-venv
	venv/bin/pip install cassandra-driver
	
install-ccm: install-venv
	venv/bin/pip install ccm

start-cassandra: install-ccm	
	mkdir -p .ccm
	venv/bin/ccm status || venv/bin/ccm create pyspark_cassandra_test -v $(CASSANDRA_VERSION) -n 1 -s && venv/bin/ccm start
	
stop-cassandra:
	venv/bin/ccm remove

test: test-python test-scala test-integration

test-python:

test-scala:

test-integration: test-integration-setup test-integration-matrix test-integration-teardown
	
test-integration-setup: start-cassandra

test-integration-teardown: stop-cassandra
	
test-integration-matrix: \
	install-cassandra-driver \
	test-integration-spark-2.0.2 \
	test-integration-spark-2.1.2 \
	test-integration-spark-2.2.0

test-travis: install-cassandra-driver
	$(call test-integration-for-version,$$SPARK_VERSION,$$SPARK_PACKAGE_TYPE)

test-integration-spark-2.2.0:
	$(call test-integration-for-version,2.2.0,hadoop2.7)

test-integration-spark-2.1.2:
	$(call test-integration-for-version,2.1.2,hadoop2.7)

test-integration-spark-2.0.2:
	$(call test-integration-for-version,2.0.2,hadoop2.7)

define test-integration-for-version
	echo ======================================================================
	echo testing integration with spark-$1
	
	mkdir -p lib && test -d lib/spark-$1-bin-$2 || \
		(pushd lib && curl http://mirrors.ocf.berkeley.edu/apache/spark/spark-$1/spark-$1-bin-$2.tgz | tar xz && popd)
	
	cp log4j.properties lib/spark-$1-bin-$2/conf/

	source venv/bin/activate ; \
		lib/spark-$1-bin-$2/bin/spark-submit \
			--master local[*] \
			--driver-memory 512m \
			--conf spark.cassandra.connection.host="localhost" \
			--jars target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra-assembly-$(VERSION).jar \
			--py-files target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra-assembly-$(VERSION).jar \
			python/pyspark_cassandra/tests.py
			
	echo ======================================================================
endef

dist: clean-pyc
	sbt -batch assembly
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra-assembly-$(VERSION).jar -@

all: clean lint dist

publish: clean
	# use spark packages to create the distribution
	sbt -batch spDist

	# push the python source files into the jar
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra_$(SCALA_TARGET_VERSION)-$(VERSION).jar -@

	# copy it to the right name, and update the jar in the zip
	cp target/scala-$(SCALA_TARGET_VERSION)/pyspark-cassandra{_$(SCALA_TARGET_VERSION),}-$(VERSION).jar
	cd target/scala-$(SCALA_TARGET_VERSION) ;\
		zip ../pyspark-cassandra-$(VERSION).zip pyspark-cassandra-$(VERSION).jar

	# send the package to spark-packages
	spark-package publish -c ".sp-creds.txt"  -n "anguenot/pyspark-cassandra" -v "$(VERSION)" -f . -z target/pyspark-cassandra-$(VERSION).zip

lint: python-tox scala-style

python-tox: ## check style with flake8
	tox

scala-style: ## check style with scalastyle
	sbt -batch scalastyle
