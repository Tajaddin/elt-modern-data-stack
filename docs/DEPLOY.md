# Deploying to a VM

The pipeline runs on one small VM: Docker Compose with Airflow 3.3 (LocalExecutor), Postgres, the
observability profile, and Traefik for TLS and rate limiting. Terraform creates the VM; GitHub Actions
deploys the application over SSH after every green CI run on `main`. Cost on Oracle's Always Free
Ampere A1 tier: $0.

## One-time setup

1. **Oracle Cloud account** with an API signing key (Profile > API keys). Copy `infra/oci/terraform.tfvars.example` to `infra/oci/terraform.tfvars` and fill in `tenancy_ocid`, `user_ocid`, `fingerprint`, `private_key_path`, `compartment_ocid`, `ssh_public_key`.
2. **Create the VM**: `make deploy` (runs `terraform init && terraform apply` in `infra/oci`). The output prints the public IP. If Oracle reports "Out of capacity" for the A1 shape, retry later or pick another availability domain (`availability_domains[1]` in `main.tf`).
3. **DNS**: point `airflow.<domain>`, `grafana.<domain>`, `prometheus.<domain>` and `alertmanager.<domain>` at that IP. A free DuckDNS name works: `yourname.duckdns.org` resolves all its subdomains to the same address.
4. **GitHub secrets** (repository > Settings > Environments > `production`):
   `VM_HOST`, `VM_SSH_KEY` (private key matching `ssh_public_key`), `DOMAIN`, `ACME_EMAIL`,
   `TRAEFIK_BASIC_AUTH` (output of `htpasswd -nB admin`, with every `$` doubled),
   `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`,
   `AIRFLOW_DB_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, optionally `SLACK_WEBHOOK_URL` and the
   `DATABRICKS_*` values. Repository variable `DBT_TARGET` selects the warehouse (`duckdb` by default).
5. **First deploy**: Actions > deploy > Run workflow. Later deploys happen automatically after CI succeeds on `main`.

## Day to day

- Airflow UI: `https://airflow.<domain>` (FAB login with `AIRFLOW_ADMIN_USER`).
- Grafana: `https://grafana.<domain>`; Prometheus and Alertmanager sit behind Traefik basic auth.
- Logs: `ssh ubuntu@<ip>`, then `cd /opt/ghcn/app && docker compose -f airflow/docker-compose.yml -f airflow/docker-compose.prod.yml logs -f airflow-scheduler`.
- Tear down everything: `make destroy` (Terraform destroys the VM, VCN and volumes; nothing else was created).

## What the VM runs

```
traefik (80/443, Let's Encrypt, rate limit, security headers)
  airflow-apiserver  <- airflow.<domain>
  grafana            <- grafana.<domain>
  prometheus         <- prometheus.<domain>  (basic auth)
  alertmanager       <- alertmanager.<domain> (basic auth)
airflow-scheduler, airflow-dag-processor, airflow-triggerer, postgres
statsd-exporter, otel-collector, tempo
```

Memory on the 2 OCPU / 12 GB shape after the stack is up is about 5 GB; the 4 GB swap file covers image builds.

## Security notes

- Secrets reach the VM only through the deploy workflow's `.env` write; nothing is committed. Rotate by updating the GitHub secret and re-running the workflow.
- SSH is key-only (Oracle's images disable passwords); `fail2ban` bans brute-force sources; `allowed_ssh_cidr` can be narrowed once the runner IP ranges are known.
- Airflow uses the FAB auth manager (hashed passwords in Postgres). Traefik rate-limits every host at 50 requests/s per client with a burst of 100.
- Unattended security upgrades reboot the VM at 04:30 UTC when needed; compose services are `restart: always`, and the daily run is at 07:00 UTC.
