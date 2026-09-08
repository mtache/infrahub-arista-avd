FROM semaphoreui/semaphore:v2.17.12

USER root

RUN apk add --no-cache python3 py3-pip && \
    pip3 install --break-system-packages \
      "anta>=1.8.0" \
      "aristaproto>=0.1.1" \
      "deepmerge>=1.1.0" \
      "distlib>=0.3.9" \
      "grpclib>=0.4.8" \
      infrahub-sdk \
      "jinja2>=3.0" \
      "netaddr>=0.7.19" \
      "pyavd==6.3.0" \
      "pyavd-utils==0.0.6" \
      "python-socks[asyncio]>=2.7.2" \
      "pyyaml>=6.0.0" \
      "requests>=2.27.0"

USER semaphore
RUN mkdir -p /opt/semaphore/collections
RUN ansible-galaxy collection install \
      opsmill.infrahub \
      arista.avd:6.3.0 \
      -p /opt/semaphore/collections
