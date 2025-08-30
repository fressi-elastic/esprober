
makefile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
src_dir := $(patsubst %/,%,$(dir $(makefile_path)))

venv_dir := $(src_dir)/.venv
venv_python := $(venv_dir)/bin/python3
venv_command = $(SHELL) -c ". $(venv_dir)/bin/activate; $(1)"

$(venv_python):
	rm -fR "$(venv_dir)"
	virtualenv "$(venv_dir)"
	$(call venv_command,$(venv_python) -m pip install --upgrade -r $(src_dir)/requirements.txt)

venv: $(venv_python)

clean-venv:
	rm -fR "$(venv_dir)"

.PHONY: venv clean_venv
