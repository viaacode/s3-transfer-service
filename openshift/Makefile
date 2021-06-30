.ONESHELL:
SHELL = /bin/bash

.PHONY: all test

test:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false
	poetry install --no-root
	export $(shell cat .env.example | xargs)
	poetry run pytest -vvv --cov=./app --junit-xml=./tests/test_results.xml