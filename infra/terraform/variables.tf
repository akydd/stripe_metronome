variable "region" {
  description = "AWS region (t4g free trial is available in most regions)."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name tag / prefix for the instance and security group."
  type        = string
  default     = "stripe-metronome-demo"
}

variable "instance_type" {
  description = "Instance type. t4g.small (2 GB, arm64) is free via the EC2 T4g trial through 2026 and matches the memory-tuned stack."
  type        = string
  default     = "t4g.small"
}

variable "disk_gb" {
  description = "Root gp3 volume size in GB."
  type        = number
  default     = 20
}

variable "repo_url" {
  description = "Optional git URL to clone and `docker compose up` on boot. Leave empty to only install Docker + swap and deploy manually."
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repo (owner/name) whose pushes may deploy via OIDC + SSM."
  type        = string
  default     = "akydd/stripe_metronome"
}

variable "deploy_branch" {
  description = "Branch whose pushes may deploy."
  type        = string
  default     = "main"
}
