.PHONY: install-tools
install-tools:
	pip install flake8

.PHONY: lint
lint:
	flake8 --max-line-length=100 ./dags ./plugins
