.PHONY: dev run
DAGSTER_HOME = $(shell pwd -P)/.dagster_home/

demo:
	uv run python main.py

dev:
	DAGSTER_HOME=$(DAGSTER_HOME) uv run dg dev -f main.py

run:
	DAGSTER_HOME=$(DAGSTER_HOME) uv run dg launch -f main.py --assets '*'
