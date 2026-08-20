.PHONY: run

run:
	DAGSTER_HOME=.dagster_home/ uv run dg launch --help
