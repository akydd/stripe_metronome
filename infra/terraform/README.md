# Terraform — demo instance

Provisions a single `t4g.small` (Amazon Linux 2023, arm64) that runs the whole
docker-compose stack. Free compute via the EC2 T4g trial through Dec 31 2026
(~$3.60/mo for the public IPv4 is the only real cost).

## Prerequisites
- Terraform ≥ 1.5 and AWS credentials configured (`aws configure` / env vars).
- For shell access: the AWS CLI **Session Manager plugin** (`session-manager-plugin`).
  There is no SSH and no key pair — the instance is managed via AWS SSM.

## Use
```sh
terraform init
terraform apply         # optionally set repo_url; all vars have defaults
```

Terraform prints the app URL, the instance id, and ready-to-run `ssm_session` /
`console_tunnel` commands.

- If you set `repo_url`, the box **self-deploys** on boot (clone + `docker compose
  up --build -d`). Give it a few minutes for the first build, then open the app URL.
- If you leave `repo_url` empty, the box comes up with Docker + swap ready; then
  get the code on it and run compose yourself over an SSM session:
  ```sh
  aws ssm start-session --target <instance_id>
  sudo dnf install -y git && git clone <repo> app && cd app && docker compose up --build -d
  ```

## Tear down (stop billing)
```sh
terraform destroy
```

## Notes
- This is a single-box demo (in-memory state); see the top-level README's
  *Production scaling* section.
- A cron job (`/etc/cron.d/reset-demo`) recreates the stack every day at
  **00:00 UTC** (`docker compose down && up -d`), wiping all demo state —
  customers, generators, topics and messages — back to a clean slate. Logs to
  `/var/log/reset-demo.log`. Trigger it by hand with
  `sudo /usr/local/bin/reset-demo.sh`.
- **No open SSH.** Only port 80 (the app, including the read-only **Live events**
  feed) is exposed publicly; management is via AWS SSM (Session Manager), which
  connects outbound over 443, so there's no port 22 and no dependency on a static
  source IP. Shell in with the `ssm_session` output. The Redpanda Console is bound
  to localhost on the host — reach it by forwarding its port over SSM with the
  `console_tunnel` output, then open <http://localhost:8080>.
