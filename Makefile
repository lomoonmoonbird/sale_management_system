install:
	@pip3 install -U pip3
	@pip3 install flake8 isort

re:
	@echo '-----------Regex-----------'
	@isort  -rc demo
	@isort *.py
	@flake8 --ignore=E501 *.py