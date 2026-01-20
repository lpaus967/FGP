# S3 Bucket for Flow Percentile Monitor
# Stores reference statistics, live output, and flood thresholds

resource "aws_s3_bucket" "flow_data" {
  bucket = "${var.project_prefix}-${var.bucket_name}-${var.environment}"

  # Safety: Terraform will only create NEW buckets, never adopt existing ones
  # If a bucket with this name exists, terraform apply will fail (safe behavior)

  tags = {
    Name        = "Flow Percentile Data"
    Environment = var.environment
    ManagedBy   = "terraform-fgp"  # Clear ownership tag
  }

  lifecycle {
    # Prevent accidental deletion of the bucket
    prevent_destroy = false  # Set to true in production for extra safety
  }
}

# Bucket versioning (recommended for data protection)
resource "aws_s3_bucket_versioning" "flow_data" {
  bucket = aws_s3_bucket.flow_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "flow_data" {
  bucket = aws_s3_bucket.flow_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Public access block - allow public read for live_output only via bucket policy
resource "aws_s3_bucket_public_access_block" "flow_data" {
  bucket = aws_s3_bucket.flow_data.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = false  # Allow bucket policy for public read
  restrict_public_buckets = false  # Allow public read via policy
}

# Bucket policy - public read for live_output prefix only
resource "aws_s3_bucket_policy" "flow_data_public_read" {
  bucket = aws_s3_bucket.flow_data.id

  # Depends on public access block being configured first
  depends_on = [aws_s3_bucket_public_access_block.flow_data]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadLiveOutput"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.flow_data.arn}/live_output/*"
      }
    ]
  })
}

# CORS configuration for frontend browser access
resource "aws_s3_bucket_cors_configuration" "flow_data" {
  bucket = aws_s3_bucket.flow_data.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]  # Restrict to your domain in production
    expose_headers  = ["ETag"]
    max_age_seconds = 3600
  }
}

# Lifecycle rules for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "flow_data" {
  bucket = aws_s3_bucket.flow_data.id

  # Archive old history files after 30 days, delete after 90
  rule {
    id     = "archive-history"
    status = "Enabled"

    filter {
      prefix = "live_output/history/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    expiration {
      days = 90
    }
  }

  # Keep logs for 30 days
  rule {
    id     = "cleanup-logs"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    expiration {
      days = 30
    }
  }
}

# Create folder structure using empty objects
resource "aws_s3_object" "folders" {
  for_each = toset([
    "reference_stats/",
    "flood_thresholds/",
    "live_output/",
    "live_output/history/",
    "logs/"
  ])

  bucket  = aws_s3_bucket.flow_data.id
  key     = each.value
  content = ""
}

# Outputs
output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.flow_data.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.flow_data.arn
}

output "bucket_regional_domain" {
  description = "Regional domain name of the bucket"
  value       = aws_s3_bucket.flow_data.bucket_regional_domain_name
}

output "live_output_url" {
  description = "Public URL for live output JSON"
  value       = "https://${aws_s3_bucket.flow_data.bucket_regional_domain_name}/live_output/current_status.json"
}
