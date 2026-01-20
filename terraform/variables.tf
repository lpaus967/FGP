variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_prefix" {
  description = "Unique prefix for all resources to avoid conflicts with existing infrastructure"
  type        = string
  default     = "fgp"  # Change this if it conflicts with existing resources
}

variable "bucket_name" {
  description = "Name of the S3 bucket for flow data"
  type        = string
  default     = "flow-percentile-data"
}
