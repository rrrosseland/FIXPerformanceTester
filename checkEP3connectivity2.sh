# 1) Get role name (if present)
ROLE=$(curl -s $(H) http://169.254.169.254/latest/meta-data/iam/security-credentials/ || true)
echo "ROLE: $ROLE"

# 2) If you got a role name, fetch creds
if [ -n "$ROLE" ]; then
  CREDS=$(curl -s $(H) http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE)
  export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | grep -oP '(?<="AccessKeyId": ")[^"]+')
  export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | grep -oP '(?<="SecretAccessKey": ")[^"]+')
  export AWS_SESSION_TOKEN=$(echo "$CREDS" | grep -oP '(?<="Token": ")[^"]+')
  export AWS_DEFAULT_REGION=$(curl -s $(H) http://169.254.169.254/latest/meta-data/placement/region)
fi

# 3) Now try AWS CLI calls (works only if the role has permission)
#    a) show your instance’s SGs & subnet
IID=$(curl -s $(H) http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 describe-instances --instance-ids "$IID" \
  --query 'Reservations[].Instances[].[InstanceId,PrivateIpAddress,PublicIpAddress,SubnetId,SecurityGroups]' --output table

#    b) dump the SG rules (replace sg-ids as printed above)
# aws ec2 describe-security-groups --group-ids sg-xxxxxxxx sg-yyyyyyyy --output table

#    c) show the NACL associated to your subnet
# SUBNET_ID=...  # from IMDS earlier
# aws ec2 describe-network-acls --filters Name=association.subnet-id,Values=$SUBNET_ID --output table
