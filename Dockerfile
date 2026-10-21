FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/kisschat-venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
RUN pip install --no-cache-dir uv==0.8.13 \
    && uv sync --locked --no-dev --no-install-project
COPY kisschat ./kisschat
RUN uv sync --locked --no-dev --no-editable
RUN useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin kisschat \
    && mkdir -p /home/kisschat/.local/share/kisschat \
    && chown -R kisschat:kisschat /home/kisschat
ENV HOME=/home/kisschat \
    PATH="/opt/kisschat-venv/bin:${PATH}"
COPY docker-entrypoint.py /usr/local/bin/kisschat-entrypoint
RUN chmod 755 /usr/local/bin/kisschat-entrypoint
ENTRYPOINT ["/usr/local/bin/kisschat-entrypoint"]
