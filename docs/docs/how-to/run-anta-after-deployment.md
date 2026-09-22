---
title: Run ANTA after deployment
description: Validate one deployed fabric from its merged Infrahub ANTA catalogs by using Semaphore.
audience: user
---

# Run ANTA after deployment

Use the **Validate with ANTA** Semaphore task after a generated EOS configuration has been merged
into Infrahub `main` and deployed to the devices. The task is read-only against EOS: it fetches the
merged **AVD ANTA Catalog** artifacts and validates one fabric through eAPI. It does not deploy or
change configuration.

## One-time setup

Build and start the stack, then register the Semaphore resources:

```bash
uv run invoke build
uv run invoke start
uv run invoke init-semaphore
```

The Semaphore image contains `arista.avd` and matching PyAVD requirements at version `6.3.0`.
`init-semaphore` creates the **Validate with ANTA** task, attaches the Infrahub dynamic inventory,
and configures `/opt/semaphore/clab-staging/anta` as the report workspace. On the Docker host, the
same files are under `lab/clab-staging/anta/`.

## 1. Prepare and merge the artifacts

Before deployment:

1. Enable `anta_enabled` on the target `NetworkFabric`.
2. Run the generator chain on a working branch.
3. Confirm every target device has a populated **AVD EOS Configuration** and **AVD ANTA Catalog**.
4. Review the proposed change and merge it into `main`.

The Semaphore task intentionally reads only `main`. It cannot validate branch artifacts before merge.

## 2. Deploy the merged configuration

Deploy the merged EOS configuration through CloudVision and wait for its change control to finish.
Direct deployment with `arista.avd.eos_config_deploy_eapi` is a future option and is not implemented
in this repository.

Do not start ANTA while a CloudVision workspace or change control is still pending. First confirm that
the Semaphore controller can route to each management address and that HTTPS eAPI accepts the account
you plan to use.

## 3. Configure EOS credentials

Choose one credential source. Values configured in the Semaphore environment take precedence over
container environment variables.

### Option A: configure the Semaphore environment

In Semaphore, open **Environment → ANTA** and add these extra variables:

```json
{
  "anta_user": "admin",
  "anta_password": "replace-with-the-device-password"
}
```

For an EOS account configured with `nopassword`, keep `anta_password` present with an empty string.
Never add credentials to the repository inventory or playbook.

### Option B: provide environment variables

Export the variables before creating or recreating the Semaphore container:

```bash
export ANTA_USER=admin
export ANTA_PASSWORD='<device-password>'
uv run invoke start
```

Use `export ANTA_PASSWORD=''` when the EOS account explicitly permits remote login without a password.
Restarting an existing container does not replace its environment; recreate it after changing these
values. Infrahub access separately uses `INFRAHUB_ADDRESS` and `INFRAHUB_API_TOKEN`.

## 4. Select and validate one fabric

1. In Semaphore, open **Environment → ANTA**.
2. Set `fabric_name` to the exact `NetworkFabric` name from Infrahub, for example
   `Fabric-L3LS-Multi-Domain`.
3. Open **Task Templates → Validate with ANTA** and start the task.
4. In the first task output, verify the reported fabric and complete device list before the ANTA tests
   begin.

The playbook selects inventory hosts whose `pod.parent` matches that fabric, rejects devices without
an Infrahub ID or management address, and fetches one catalog per selected device from `main`.

## 5. Review the result

The task fails when no tests execute, a device or artifact cannot be read, ANTA reports an execution
error, or any test fails. A successful task means every executed test passed; skipped tests remain
visible in the reports.

Each run writes an isolated timestamped directory containing:

- `reports/anta_report.json`
- `reports/anta_report.md`
- `reports/anta_report.csv`
- `user_catalogs/<device>.yml`

The task output prints the container paths. With the bundled Compose stack, find the same run under
`lab/clab-staging/anta/<fabric>-<timestamp>/` on the Docker host.

## Troubleshooting

| Symptom | Cause and action |
|---|---|
| No devices found for the fabric | Check the exact `fabric_name`. Devices must have a pod whose parent is that fabric on Infrahub `main`. |
| Device is missing its ID or management address | Populate the device's `mgmt_ip` relationship and regenerate before retrying. |
| Catalog does not contain ANTA tests | Enable `anta_enabled`, generate structured configurations and catalogs, then merge the refreshed artifacts into `main`. Marker-only catalogs are rejected. |
| Credentials are missing | Configure both role-native Semaphore variables or provide both `ANTA_USER` and `ANTA_PASSWORD`. An empty password must still be explicitly supplied. |
| `401 Unauthorized` | Confirm the deployed EOS account and password match the selected credentials and permit eAPI login. |
| Connection refused, timeout, or no route to host | Verify management routing, HTTPS eAPI, access control lists, and that configuration deployment or CloudVision change control has completed. |
| No tests executed | Confirm each fetched catalog contains tests tagged for its device and that every selected hostname matches the merged artifact target. |

For catalog-generation problems, see [Troubleshooting](../troubleshooting.md#the-anta-catalog-artifact-contains-only-a-comment).
