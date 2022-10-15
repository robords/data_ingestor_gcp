.PHONY: setup install
# A phony target is one that is not really the name of a file; rather it is just a name for a recipe to be executed when you make an explicit request. There are two reasons to use a phony target: to 
# avoid a conflict with a file of the same name, and to improve performance.

all: setup

setup:
	python3 -m venv ~/.venv/gc_cs_data_ingestor
# https://docs.python.org/3/library/venv.html#creating-virtual-environments

install:
	pip install -r requirements.txt
