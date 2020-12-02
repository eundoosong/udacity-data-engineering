.PHONY: install-tools
install-tools:
	pip install flake8

.PHONY: lint
lint:
	flake8 --max-line-length=100 ./dags ./plugins

.PHONY: clean
clean:
	@find dags "(" -name "*.pyc" -o -name __pycache__ ")" -delete
	@find plugins "(" -name "*.pyc" -o -name __pycache__ ")" -delete
	@find . "(" -name "*.pyc" -o -name __pycache__ ")" -delete
