# --- Continuous deployment ---
# Lets a GitHub Actions workflow in this repo deploy to the instance via SSM,
# authenticating with short-lived OIDC credentials (no long-lived AWS keys stored
# in GitHub). The workflow assumes the role below, then sends one SSM RunShellScript
# command telling the box to pull the latest code and `docker compose up --build`.

data "aws_caller_identity" "current" {}

# GitHub's OIDC identity provider. One per AWS account per URL — if your account
# already has this provider, remove this resource and reference the existing one.
data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS no longer validates this for the GitHub provider, but the argument is
  # still required; the tls data source keeps it correct without hardcoding.
  thumbprint_list = [data.tls_certificate.github.certificates[length(data.tls_certificate.github.certificates) - 1].sha1_fingerprint]
}

# The role the workflow assumes. Trust is scoped to pushes on one branch of one
# repo, so nothing else (other repos, PRs, forks) can assume it.
data "aws_iam_policy_document" "cd_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # AWS requires the GitHub OIDC trust to be scoped by `sub` (or job_workflow_ref).
    # This account uses GitHub's immutable subject claims, so `sub` is
    # repo:<owner>@<owner_id>/<name>@<repo_id>:ref:refs/heads/<branch>. Wildcard the
    # numeric IDs (immutable anyway) and pin owner, repo and branch.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${split("/", var.github_repo)[0]}@*/${split("/", var.github_repo)[1]}@*:ref:refs/heads/${var.deploy_branch}"]
    }
  }
}

resource "aws_iam_role" "cd" {
  name               = "${var.name}-cd"
  assume_role_policy = data.aws_iam_policy_document.cd_assume.json
  tags               = { Name = "${var.name}-cd" }
}

# Least privilege: send a RunShellScript command to the tagged instance and read
# the result back. Nothing else — no shell keys, no broad SSM access.
data "aws_iam_policy_document" "cd_deploy" {
  statement {
    sid       = "SendCommandDocument"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ssm:${var.region}::document/AWS-RunShellScript"]
  }
  statement {
    sid       = "SendCommandInstance"
    actions   = ["ssm:SendCommand"]
    resources = ["arn:aws:ec2:${var.region}:${data.aws_caller_identity.current.account_id}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ssm:resourceTag/Name"
      values   = [var.name]
    }
  }
  statement {
    sid       = "TrackCommand"
    actions   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ec2:DescribeInstances"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cd_deploy" {
  name   = "${var.name}-cd-deploy"
  role   = aws_iam_role.cd.id
  policy = data.aws_iam_policy_document.cd_deploy.json
}
