FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY data ./data
RUN pip install --no-cache-dir '.[stream]'
RUN useradd --create-home appuser
USER appuser
CMD ["kg-lab", "stream-consume"]
