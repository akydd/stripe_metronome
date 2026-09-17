output "public_ip" {
  description = "Public IPv4 of the demo instance."
  value       = aws_instance.demo.public_ip
}

output "app_url" {
  description = "The billing console (nginx)."
  value       = "http://${aws_instance.demo.public_ip}/"
}

output "instance_id" {
  description = "EC2 instance id (used as the --target for SSM sessions)."
  value       = aws_instance.demo.id
}

output "ssm_session" {
  description = "Open a shell on the instance via SSM (no SSH, no key). Needs the Session Manager plugin."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.demo.id}"
}

output "github_actions_role_arn" {
  description = "Set this as the GitHub repo variable AWS_DEPLOY_ROLE_ARN for the CD workflow."
  value       = aws_iam_role.cd.arn
}

output "console_tunnel" {
  description = "Forward the localhost-bound Redpanda Console over SSM, then open http://localhost:8080."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.demo.id} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
}
