.ONESHELL:
SHELL = /bin/bash

.PHONY: all test

test:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
	source $$HOME/.poetry/env
	poetry install --no-root
	export $(shell cat .env.example | xargs)
	poetry run pytest -vvv --cov=./app --junit-xml=./tests/test_results.xml