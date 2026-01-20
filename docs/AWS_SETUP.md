# AWS Infrastructure Setup Guide

This document provides step-by-step instructions for deploying the Flow Percentile Monitor infrastructure on AWS using Terraform.

## Prerequisites

- AWS CLI installed and configured
- Terraform >= 1.0 installed
- Access to an AWS account with permissions to create IAM roles, S3 buckets, and EC2 instances

---

## 1. AWS CLI Configuration

### 1.1 Configure Named Profile

If you have multiple AWS accounts, use a named profile:

```bash
aws configure --profile flow-percentile
```

Enter when prompted:
- **AWS Access Key ID**: Your access key
- **AWS Secret Access Key**: Your secret key
- **Default region**: `us-east-1`
- **Default output format**: `json`

### 1.2 Set Profile for Session

```bash
export AWS_PROFILE=flow-percentile
```

### 1.3 Verify Connection

```bash
aws sts get-caller-identity
```

Expected output:
```json
{
    "UserId": "AIDAXXXXXXXXXX",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-username"
}
```

---

## 2. Terraform Deployment

### 2.1 Directory Structure

```
terraform/
├── providers.tf      # AWS provider configuration
├── variables.tf      # Input variables
├── iam.tf           # IAM policy for Terraform operations
├── iam_ec2.tf       # IAM role for EC2 instances
├── s3.tf            # S3 bucket configuration
├── terraform.tfvars.example
└── .gitignore
```

### 2.2 Configure Variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:
```hcl
aws_region  = "us-east-1"
environment = "dev"           # or "staging", "prod"
bucket_name = "flow-percentile-data"
```

### 2.3 Initialize Terraform

```bash
terraform init
```

### 2.4 Preview Changes

```bash
terraform plan
```

Review the output to ensure it will create:
- S3 bucket: `flow-percentile-data-{environment}`
- IAM role: `flow-percentile-ec2-role-{environment}`
- IAM instance profile: `flow-percentile-ec2-profile-{environment}`
- IAM policy for Terraform user
- S3 bucket policy, CORS, lifecycle rules

### 2.5 Deploy Infrastructure

```bash
terraform apply
```

Type `yes` when prompted.

### 2.6 Save Outputs

After successful deployment, note these outputs:
```
bucket_name               = "flow-percentile-data-dev"
bucket_arn                = "arn:aws:s3:::flow-percentile-data-dev"
live_output_url           = "https://flow-percentile-data-dev.s3.us-east-1.amazonaws.com/live_output/current_status.json"
ec2_instance_profile_name = "flow-percentile-ec2-profile-dev"
ec2_role_arn              = "arn:aws:iam::XXXX:role/flow-percentile-ec2-role-dev"
```

---

## 3. S3 Bucket Structure

The Terraform creates this folder structure:

```
flow-percentile-data-dev/
├── reference_stats/          # Pipeline A output (monthly)
│   └── state=VT/data.parquet
├── flood_thresholds/         # NWS flood thresholds (optional)
│   └── flood_thresholds.parquet
├── live_output/              # Pipeline B output (hourly) - PUBLIC
│   ├── current_status.json   # Latest snapshot
│   └── history/              # Archived hourly runs
│       └── 2026-01-15T1400.json
└── logs/                     # Application logs
```

### Access Permissions

| Prefix | Access | Purpose |
|--------|--------|---------|
| `reference_stats/` | Private | Historical percentile statistics |
| `flood_thresholds/` | Private | NWS flood stage thresholds |
| `live_output/` | **Public Read** | Frontend JSON consumption |
| `logs/` | Private | Application logs |

### Lifecycle Rules

| Prefix | Rule |
|--------|------|
| `live_output/history/` | Archive to STANDARD_IA after 30 days, Glacier after 60 days, delete after 90 days |
| `logs/` | Delete after 30 days |

---

## 4. EC2 Instance Setup

### 4.1 Launch EC2 Instance

**Via AWS Console:**
1. Go to EC2 → Launch Instance
2. Select **Ubuntu 22.04 LTS** (or Amazon Linux 2023)
3. Instance type: **t3.medium** (recommended for memory)
4. Key pair: Create or select existing
5. Network: Default VPC, allow SSH (port 22)
6. **Advanced Details → IAM instance profile**: Select `flow-percentile-ec2-profile-dev`
7. Storage: 20 GB gp3

**Via AWS CLI:**
```bash
aws ec2 run-instances \
  --image-id ami-0c7217cdde317cfec \
  --instance-type t3.medium \
  --key-name your-key-pair \
  --iam-instance-profile Name=flow-percentile-ec2-profile-dev \
  --security-group-ids sg-xxxxxxxx \
  --subnet-id subnet-xxxxxxxx \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=flow-percentile-monitor}]'
```

### 4.2 Connect to Instance

```bash
ssh -i your-key.pem ubuntu@<instance-public-ip>
```

### 4.3 Install Dependencies

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.10+
sudo apt install -y python3 python3-pip python3-venv git

# Clone repository
git clone https://github.com/your-org/FGP.git
cd FGP

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4.4 Configure Application

```bash
cp .env.example .env
nano .env
```

Set the bucket name:
```
S3_BUCKET_NAME=flow-percentile-data-dev
AWS_REGION=us-east-1
MAX_WORKERS=10
```

### 4.5 Verify S3 Access

The EC2 instance automatically gets credentials via the IAM role. Test with:

```bash
aws s3 ls s3://flow-percentile-data-dev/
```

Expected output:
```
PRE flood_thresholds/
PRE live_output/
PRE logs/
PRE reference_stats/
```

---

## 5. Running the Pipelines

### 5.1 Pipeline A: Reference Generator (Monthly)

Generates historical percentile statistics for all states:

```bash
# Single state (for testing)
python -m src.main --mode=slow --states=VT

# All states (production)
python -m src.main --mode=slow
```

**Runtime:** 2-4 hours for all states
**Output:** `s3://flow-percentile-data-dev/reference_stats/state=XX/data.parquet`

### 5.2 Pipeline B: Live Monitor (Hourly)

Calculates current conditions and uploads to S3:

```bash
# Single state (for testing)
python -m src.main --mode=fast --states=VT

# All states (production)
python -m src.main --mode=fast
```

**Runtime:** 15-30 minutes for all states
**Output:** `s3://flow-percentile-data-dev/live_output/current_status.json`

---

## 6. Crontab Setup

### 6.1 Edit Crontab

```bash
crontab -e
```

### 6.2 Add Schedules

```bash
# Pipeline B: Run hourly at minute 0
0 * * * * cd /home/ubuntu/FGP && /home/ubuntu/FGP/venv/bin/python -m src.main --mode=fast >> /home/ubuntu/FGP/logs/pipeline_b.log 2>&1

# Pipeline A: Run monthly on the 1st at midnight
0 0 1 * * cd /home/ubuntu/FGP && /home/ubuntu/FGP/venv/bin/python -m src.main --mode=slow >> /home/ubuntu/FGP/logs/pipeline_a.log 2>&1
```

### 6.3 Verify Crontab

```bash
crontab -l
```

### 6.4 Create Log Directory

```bash
mkdir -p /home/ubuntu/FGP/logs
```

---

## 7. Monitoring & Troubleshooting

### 7.1 Check Pipeline Logs

```bash
# Recent Pipeline B runs
tail -100 /home/ubuntu/FGP/logs/pipeline_b.log

# Recent Pipeline A runs
tail -100 /home/ubuntu/FGP/logs/pipeline_a.log
```

### 7.2 Verify S3 Uploads

```bash
# Check latest upload time
aws s3 ls s3://flow-percentile-data-dev/live_output/current_status.json

# Download and inspect
aws s3 cp s3://flow-percentile-data-dev/live_output/current_status.json - | head -50
```

### 7.3 Check Public Access

```bash
curl -s https://flow-percentile-data-dev.s3.us-east-1.amazonaws.com/live_output/current_status.json | head -20
```

### 7.4 Common Issues

| Issue | Solution |
|-------|----------|
| `NoCredentialsError` | Verify EC2 has IAM instance profile attached |
| `AccessDenied` on S3 | Check IAM policy allows the specific prefix |
| Pipeline timeout | Increase `MAX_WORKERS` or use larger instance |
| Missing reference data | Run Pipeline A before Pipeline B |

---

## 8. Terraform State Management

### 8.1 View Current State

```bash
terraform show
```

### 8.2 Update Infrastructure

```bash
terraform plan    # Preview changes
terraform apply   # Apply changes
```

### 8.3 Destroy Infrastructure

```bash
terraform destroy
```

**Warning:** This deletes all resources including S3 bucket and data.

---

## 9. Cost Optimization

### Estimated Monthly Costs

| Resource | Cost |
|----------|------|
| EC2 t3.medium (on-demand) | ~$30/month |
| S3 storage (50 GB) | ~$1.15/month |
| S3 requests | ~$0.50/month |
| Data transfer | ~$1-5/month |
| **Total** | **~$35-40/month** |

### Cost Saving Tips

1. Use **Spot Instances** for Pipeline A (batch processing)
2. Use **Reserved Instances** for production EC2
3. Lifecycle rules automatically archive/delete old data
4. Consider **Lambda** for Pipeline B if runs are short

---

## 10. Security Checklist

- [ ] IAM roles use least-privilege permissions
- [ ] S3 bucket has encryption enabled
- [ ] Only `live_output/` prefix is public
- [ ] EC2 security group restricts SSH to known IPs
- [ ] No AWS credentials stored in code or .env files
- [ ] Terraform state does not contain secrets

---

## Quick Reference

### Key URLs

| Resource | URL/Command |
|----------|-------------|
| Live JSON | `https://flow-percentile-data-dev.s3.us-east-1.amazonaws.com/live_output/current_status.json` |
| S3 Console | `https://s3.console.aws.amazon.com/s3/buckets/flow-percentile-data-dev` |
| EC2 Console | `https://console.aws.amazon.com/ec2/` |

### Key Commands

```bash
# Test Pipeline B locally
python -m src.main --mode=fast --states=VT

# Check S3 bucket contents
aws s3 ls s3://flow-percentile-data-dev/ --recursive

# View live output
aws s3 cp s3://flow-percentile-data-dev/live_output/current_status.json -

# Check EC2 IAM role
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

---

## Appendix: Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `S3_BUCKET_NAME` | S3 bucket for all data | `flow-percentile-data-dev` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `MAX_WORKERS` | Parallel fetch workers | `10` |
| `AWS_PROFILE` | (Local only) Named AWS profile | `flow-percentile` |
