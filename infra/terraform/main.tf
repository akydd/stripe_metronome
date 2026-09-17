terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Amazon Linux 2023 (arm64) — matches the t4g (Graviton) instance.
data "aws_ami" "al2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_security_group" "demo" {
  name        = "${var.name}-sg"
  description = "stripe_metronome demo: app only (management via SSM, no open SSH)"

  # Only the app is exposed. There is NO SSH ingress: the instance is managed via
  # AWS Systems Manager (Session Manager), which connects outbound over 443 (the
  # egress rule below) — so no port 22, and no dependency on a static source IP.
  ingress {
    description = "App (nginx). Includes the read-only Live events feed."
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # The Redpanda Console is bound to localhost on the host (its UI can create /
  # delete topics), so no public ingress for :8080 — reach it via SSM port
  # forwarding (see the console_tunnel output).
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name}-sg" }
}

# --- SSM (Session Manager) access: an IAM role the instance assumes so it can
# register with Systems Manager. No inbound SSH, no key pair. ---
data "aws_iam_policy_document" "ssm_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ssm" {
  name               = "${var.name}-ssm"
  assume_role_policy = data.aws_iam_policy_document.ssm_assume.json
  tags               = { Name = "${var.name}-ssm" }
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ssm" {
  name = "${var.name}-ssm"
  role = aws_iam_role.ssm.name
}

resource "aws_instance" "demo" {
  ami                         = data.aws_ami.al2023_arm64.id
  instance_type               = var.instance_type
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  vpc_security_group_ids      = [aws_security_group.demo.id]
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp3"
    volume_size = var.disk_gb
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url = var.repo_url
  })

  # user_data (cloud-init) only runs once, at first boot — editing it later has no
  # effect on a running instance, and applying such a change would needlessly
  # stop/start the box (and change its public IP). Ignore in-place user_data
  # changes; a deliberate instance *replacement* still picks up the current script.
  lifecycle {
    ignore_changes = [user_data]
  }

  tags = { Name = var.name }
}
