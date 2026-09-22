FROM semaphoreui/semaphore:v2.17.12

USER root

RUN apk add --no-cache python3 py3-pip && \
    pip3 install --break-system-packages \
      infrahub-sdk \
      "pyavd[ansible]==6.3.0"

USER semaphore
RUN mkdir -p /opt/semaphore/collections
RUN ansible-galaxy collection install \
      opsmill.infrahub:1.9.0 \
      arista.avd:6.3.0 \
      -p /opt/semaphore/collections
