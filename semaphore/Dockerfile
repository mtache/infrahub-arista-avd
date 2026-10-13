FROM semaphoreui/semaphore:v2.17.12

ARG SEMAPHORE_UID=1000
ARG SEMAPHORE_GID=1000

ENV ANSIBLE_COLLECTIONS_PATH=/opt/semaphore/collections:/usr/share/ansible/collections

USER root

RUN apk add --no-cache python3 py3-pip shadow && \
    case "${SEMAPHORE_UID}" in ''|*[!0-9]*) echo "SEMAPHORE_UID must be a positive integer" >&2; exit 1 ;; esac && \
    case "${SEMAPHORE_GID}" in ''|*[!0-9]*) echo "SEMAPHORE_GID must be a positive integer" >&2; exit 1 ;; esac && \
    [ "${SEMAPHORE_UID}" -gt 0 ] && [ "${SEMAPHORE_GID}" -gt 0 ] && \
    [ "${SEMAPHORE_UID}" -le 2147483647 ] && [ "${SEMAPHORE_GID}" -le 2147483647 ] && \
    existing_user="$(awk -F: -v uid="${SEMAPHORE_UID}" '$3 == uid { print $1; exit }' /etc/passwd)" && \
    if [ -n "${existing_user}" ] && [ "${existing_user}" != semaphore ]; then \
      echo "SEMAPHORE_UID ${SEMAPHORE_UID} already belongs to ${existing_user}" >&2; exit 1; \
    fi && \
    group_name="$(awk -F: -v gid="${SEMAPHORE_GID}" '$3 == gid { print $1; exit }' /etc/group)" && \
    if [ -z "${group_name}" ]; then \
      group_name=semaphore-group; groupadd --gid "${SEMAPHORE_GID}" "${group_name}"; \
    fi && \
    usermod --uid "${SEMAPHORE_UID}" --gid "${SEMAPHORE_GID}" --groups "${group_name}" semaphore && \
    mkdir -p /etc/semaphore /var/lib/semaphore /tmp/semaphore /opt/semaphore/collections && \
    chown -R "${SEMAPHORE_UID}:${SEMAPHORE_GID}" \
      /home/semaphore /etc/semaphore /var/lib/semaphore /tmp/semaphore /opt/semaphore/collections && \
    chmod 0755 /home/semaphore /opt/semaphore/collections && \
    chmod 0700 /etc/semaphore /var/lib/semaphore /tmp/semaphore && \
    pip3 uninstall --yes ansible && \
    pip3 install --break-system-packages \
      "ansible-core==2.19.13" \
      infrahub-sdk \
      "pyavd[ansible]==6.4.0" && \
    pip3 check

USER semaphore
RUN ansible-galaxy collection install \
      opsmill.infrahub:1.9.0 \
      arista.avd:6.4.0 \
      -p /opt/semaphore/collections
