
makefile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
src_dir := $(patsubst %/,%,$(dir $(makefile_path)))

venv_dir := $(src_dir)/.venv
venv_python := $(venv_dir)/bin/python3
venv_sh = $(SHELL) -c ". $(venv_dir)/bin/activate; $(1)"

all: venv

clean: clean-venv

venv: $(venv_python)

run: venv
	uv run esprober.py

clean-venv:
	rm -fR "$(venv_dir)"

check-uv:
	@echo '$(or $(shell uv --version), $(error "Please install uv https://github.com/astral-sh/uv?tab=readme-ov-file#installation"))'

$(venv_python): check-uv
	uv add --dev .

.PHONY: venv run clean clean-venv check-uv
