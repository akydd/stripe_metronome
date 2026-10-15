#!/usr/bin/env bash
# Stop the demo instance after a recruiter session. While stopped you pay only for
# the EBS volume (~$1.60/mo) — no compute, no public IPv4.
set -euo pipefail
REGION="${REGION:-ca-central-1}"
NAME="${NAME:-stripe-metronome-demo}"

ID="$(aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)"
if [ -z "$ID" ] || [ "$ID" = "None" ]; then
  echo "no instance tagged Name=$NAME in $REGION" >&2
  exit 1
fi

echo "Stopping $ID ..."
aws ec2 stop-instances --region "$REGION" --instance-ids "$ID" >/dev/null
echo "Stop requested. Only the ~\$1.60/mo EBS volume bills while stopped."
