FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN uv sync --locked

EXPOSE 8000
CMD ["uv", "run", "main.py"]