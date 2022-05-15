FROM python:3.9-slim AS prod

WORKDIR /app

# RUN apt update && apt install -y \
#     python3-pip

# # install pipenv to create virtual environment
COPY Pipfile Pipfile.lock .
RUN pip install pipenv \
    && pipenv install

# FROM prod AS dev
# # COPY <insert pipenv dir with packages>
# RUN pipenv install --dev
