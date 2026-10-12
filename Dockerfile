ARG INFRAHUB_BASE_VERSION=1.11.5
FROM registry.opsmill.io/opsmill/infrahub:${INFRAHUB_BASE_VERSION}

# Extend the virtualenv shipped by Infrahub without replacing its interpreter.
ENV UV_PROJECT_ENVIRONMENT="/.venv"

WORKDIR /opt/local

COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

# --no-dev is necessary to avoid installing dev dependencies including a different version of the infrahub-sdk
# --inexact preserves Infrahub packages that are not solution dependencies.
# Selecting the inherited interpreter and disabling downloads makes an
# incompatible base image fail instead of recreating /.venv for another Python.
RUN /.venv/bin/python -c 'import sys, infrahub; assert sys.version_info[:2] == (3, 14)' && \
    uv sync --no-dev --frozen --inexact \
      --python /.venv/bin/python --no-python-downloads && \
    uv pip check --python /.venv/bin/python && \
    /.venv/bin/python -c 'import sys, infrahub, pyavd, solution_arista_avd; assert sys.version_info[:2] == (3, 14)'

WORKDIR /source
