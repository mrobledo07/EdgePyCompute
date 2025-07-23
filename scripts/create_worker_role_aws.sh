#!/bin/bash

# Creates an IAM role for an EC2 instance with s3 read/write permissions

# Define variables
ROLE_NAME="MyEC2S3AccessRole"
POLICY_NAME="MyS3AccessPolicy"
BUCKET_NAME_CLIENT="clientbucketforstoringinputs"
BUCKET_NAME_ORCH="orchestratorfororchestratingworkers" # Replace with bucket name
REGION="us-north-1"

# 1. Create a trust policy for the EC2 service
echo "Creating trust policy for EC2..."
cat <<EOF > ec2-trust-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# 2. Create the IAM role
echo "Creating IAM role: $ROLE_NAME..."
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Role '$ROLE_NAME' already exists. Skipping creation."
else
  aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file://ec2-trust-policy.json --query 'Role.Arn' --output text
  if [ $? -ne 0 ]; then
      echo "Error creating role. Exiting."
      exit 1
  fi
  echo "Role '$ROLE_NAME' created successfully."
fi

# 3. Create a custom policy for full S3 access
echo "Creating custom S3 access policy: $POLICY_NAME..."
cat <<EOF > s3-custom-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject",
        "s3:HeadObject"
      ],
      "Resource": [
        "arn:aws:s3:::$BUCKET_NAME_CLIENT",
        "arn:aws:s3:::$BUCKET_NAME_CLIENT/*",
        "arn:aws:s3:::$BUCKET_NAME_ORCH",
        "arn:aws:s3:::$BUCKET_NAME_ORCH/*"
      ]
    }
  ]
}
EOF

# Create the policy
if aws iam list-policies --scope Local --query "Policies[?PolicyName=='$POLICY_NAME']" --output text | grep -q "$POLICY_NAME"; then
  echo "Policy '$POLICY_NAME' already exists. Skipping creation."
  POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='$POLICY_NAME'].Arn" --output text)
else
  POLICY_ARN=$(aws iam create-policy --policy-name "$POLICY_NAME" --policy-document file://s3-custom-policy.json --query 'Policy.Arn' --output text)
  if [ $? -ne 0 ]; then
    echo "Error creating policy. Exiting."
    exit 1
  fi
  echo "Policy '$POLICY_NAME' created with ARN: $POLICY_ARN"
fi

# 4. Attach the policy to the role
echo "Attaching policy '$POLICY_NAME' to role '$ROLE_NAME'..."
aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$POLICY_ARN"
if [ $? -ne 0 ]; then
    echo "Error attaching policy. Exiting."
    exit 1
fi
echo "Policy attached successfully."

# 5. Create an instance profile and associate the role
# In the AWS Console, when you create an EC2 role, an instance profile is automatically created.
# With the CLI, you explicitly create it and associate the role.
echo "Creating instance profile for role: $ROLE_NAME..."
if aws iam get-instance-profile --instance-profile-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Instance profile '$ROLE_NAME' already exists. Skipping creation."
else
  aws iam create-instance-profile --instance-profile-name "$ROLE_NAME"
  if [ $? -ne 0 ]; then
    echo "Error creating instance profile. Exiting."
    exit 1
  fi
  echo "Instance profile '$ROLE_NAME' created successfully."
fi

# 6. Add the role to the instance profile
echo "Adding role '$ROLE_NAME' to instance profile '$ROLE_NAME'..."
# Check if the role is already associated with the instance profile
if aws iam get-instance-profile --instance-profile-name "$ROLE_NAME" --query "InstanceProfile.Roles[?RoleName=='$ROLE_NAME']" --output text | grep -q "$ROLE_NAME"; then
  echo "Role '$ROLE_NAME' is already associated with instance profile '$ROLE_NAME'. Skipping add."
else
  aws iam add-role-to-instance-profile --instance-profile-name "$ROLE_NAME" --role-name "$ROLE_NAME"
  if [ $? -ne 0 ]; then
    echo "Error adding role to instance profile. Exiting."
    exit 1
  fi
  echo "Role added to instance profile successfully."
fi

echo "Script completed. IAM Role '$ROLE_NAME' and Instance Profile '$ROLE_NAME' created with S3 access."

# Clean up temporary policy files
rm ec2-trust-policy.json s3-custom-policy.json
