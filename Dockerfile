FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    locales git gcc build-essential ffmpeg xvfb poppler-utils && \
    echo "pt_BR.UTF-8 UTF-8" > /etc/locale.gen && locale-gen && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*


WORKDIR /app

COPY requirements.txt requirements.txt

COPY .env /root/.env

ARG GIT_USERNAME
ARG GIT_TOKEN

RUN sed -i "s/\${GIT_USERNAME}/${GIT_USERNAME}/g" requirements.txt && \
    sed -i "s/\${GIT_TOKEN}/${GIT_TOKEN}/g" requirements.txt

RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV payload=""

ENTRYPOINT ["python", "main.py"]