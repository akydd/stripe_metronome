#!/usr/bin/env bash
# Start the demo instance for a recruiter session and print its URL. The public IP
# changes each start (a stopped instance releases it), so share the URL this prints.
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

echo "Starting $ID ..."
aws ec2 start-instances --region "$REGION" --instance-ids "$ID" >/dev/null
aws ec2 wait instance-running --region "$REGION" --instance-ids "$ID"

IP="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
URL="http://$IP/"
echo "Instance running. Public IP: $IP"
echo "Waiting for the app (Docker restarts the stack on boot)..."
for _ in $(seq 1 60); do
  if [ "$(curl -s -o /dev/null -m 5 -w '%{http_code}' "$URL" || true)" = "200" ]; then
    echo "App is up → $URL"
    exit 0
  fi
  sleep 5
done
echo "App not responding yet at $URL — give it another minute, then retry $URL." >&2
