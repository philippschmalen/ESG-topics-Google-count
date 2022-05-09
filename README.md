# Google results count for a given topic

## Objective

Track how many Google results exist for a specified keyword.

## Key results

* build pipeline with Prefect 2.0
  * daily schedule running at night
  * use pydantic to validate data
* store results in SQL database on server host
* visualize in line plot on [towards sustainable finance ](https://www.towardssustainablefinance.com)
* slack message in case of errors
* create github action that runs CICD pipeline
  * test pipeline with pytest
  * build docker image
  * push docker image to ghcr.io
  * deploy and run docker with [docker-compose on server](https://betterprogramming.pub/docker-deployments-with-github-actions-7e59bb532505)

## TODO

* migrate from `conda` to `pipenv`
* create `tsf` database
* create table `google-results-count`
* install pre-commit hooks, flake8, black, mypy