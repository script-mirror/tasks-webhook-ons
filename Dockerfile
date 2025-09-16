FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    locales \
    git \
    gcc \
    build-essential \
    ffmpeg \
    xvfb && \
    echo "pt_BR.UTF-8 UTF-8" > /etc/locale.gen && \
    locale-gen && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


WORKDIR /app

COPY requirements.txt .

ARG GIT_USERNAME
ARG GIT_TOKEN
RUN git config --global credential.helper store && \
    echo "https://${GIT_USERNAME}:${GIT_TOKEN}@github.com" > ~/.git-credentials

RUN pip install --no-cache-dir -r requirements.txt

RUN rm -f ~/.git-credentials && \
    git config --global --unset credential.helper

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
