# Use IMDSv2 token (falls back to v1 if needed)
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
H() { if [ -n "$TOKEN" ]; then echo "-H X-aws-ec2-metadata-token:$TOKEN"; fi; }

# Core identity
curl -s $(H) http://169.254.169.254/latest/meta-data/instance-id
curl -s $(H) http://169.254.169.254/latest/meta-data/local-ipv4
curl -s $(H) http://169.254.169.254/latest/meta-data/public-ipv4 || echo "no-public-ip"
curl -s $(H) http://169.254.169.254/latest/meta-data/placement/region

# Network details
MAC=$(curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/ | head -n1)
echo "MAC: $MAC"

curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/vpc-id
curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-id
curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/security-group-ids
curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/vpc-ipv4-cidr-block
curl -s $(H) http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/ipv4-associations/ # if EIP

# Your egress/public IP as seen on the internet (useful for allowlists)
curl -s https://icanhazip.com

