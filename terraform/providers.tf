terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to use S3 backend for state (recommended for team use)
  # backend "s3" {
  #   bucket         = "flow-percentile-terraform-state"
  #   key            = "terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "flow-percentile-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region  = var.aws_region
  profile = "fgp"

  default_tags {
    tags = {
      Project     = "flow-percentile-monitor"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
