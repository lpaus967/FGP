# IAM Role for EC2 instances running the Flow Percentile pipelines
# This role allows EC2 to read/write to the S3 bucket

# Trust policy - allows EC2 to assume this role
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# Policy for EC2 to access the flow data bucket
data "aws_iam_policy_document" "ec2_s3_access" {
  # Read/write reference stats
  statement {
    sid    = "ReadWriteReferenceStats"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.flow_data.arn}/reference_stats/*"
    ]
  }

  # Read/write flood thresholds
  statement {
    sid    = "ReadWriteFloodThresholds"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.flow_data.arn}/flood_thresholds/*"
    ]
  }

  # Read/write live output
  statement {
    sid    = "ReadWriteLiveOutput"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.flow_data.arn}/live_output/*"
    ]
  }

  # Write logs
  statement {
    sid    = "WriteLogs"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.flow_data.arn}/logs/*"
    ]
  }

  # List bucket contents
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.flow_data.arn
    ]
  }
}

# Create the IAM role
# Note: Role must be prefixed with "ec2-app-" per org policy
resource "aws_iam_role" "ec2_flow_pipeline" {
  name               = "ec2-app-${var.project_prefix}-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Name        = "Flow Percentile EC2 Role"
    Environment = var.environment
    ManagedBy   = "terraform-fgp"
  }
}

# Create the policy
# Note: Policy must be prefixed with "ec2-app-" per org policy
resource "aws_iam_policy" "ec2_s3_access" {
  name        = "ec2-app-${var.project_prefix}-policy-${var.environment}"
  description = "Allows EC2 instances to access Flow Percentile S3 bucket"
  policy      = data.aws_iam_policy_document.ec2_s3_access.json

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform-fgp"
  }
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "ec2_s3_access" {
  role       = aws_iam_role.ec2_flow_pipeline.name
  policy_arn = aws_iam_policy.ec2_s3_access.arn
}

# Instance profile (required to attach role to EC2)
# Note: Profile must be prefixed with "ec2-app-" per org policy
resource "aws_iam_instance_profile" "ec2_flow_pipeline" {
  name = "ec2-app-${var.project_prefix}-profile-${var.environment}"
  role = aws_iam_role.ec2_flow_pipeline.name

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform-fgp"
  }
}

# Outputs
output "ec2_role_arn" {
  description = "ARN of the EC2 IAM role"
  value       = aws_iam_role.ec2_flow_pipeline.arn
}

output "ec2_instance_profile_name" {
  description = "Name of the EC2 instance profile (use when launching EC2)"
  value       = aws_iam_instance_profile.ec2_flow_pipeline.name
}

output "ec2_instance_profile_arn" {
  description = "ARN of the EC2 instance profile"
  value       = aws_iam_instance_profile.ec2_flow_pipeline.arn
}
