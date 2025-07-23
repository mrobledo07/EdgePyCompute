aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=PyEdgeCompute-worker" \
  --query "Reservations[*].Instances[*].InstanceId" --output text)