VENV := venv

setup: $(VENV)/bin/activate

$(VENV)/bin/activate: requirements.txt
	@test -d $(VENV) || python3 -m venv $(VENV)
	@. $(VENV)/bin/activate; pip install -r requirements.txt
	@echo "Virtual environment created and dependencies installed."

