.PHONY: run
run:
	@python main.py

.PHONY: clean
clean:
	@echo "Cleaning all __pycache__, .pyc, .pyo, .pytest_cache files..."
	@bash -c 'find . | grep -E "(__pycache__|\\.pyc|\\.pyo$$|.pytest_cache)" | xargs rm -rf'