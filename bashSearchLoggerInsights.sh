export AWS_DEFAULT_REGION=us-east-1

ROLE_ARN="arn:aws:iam::536667838315:role/LogsReadRole-ForecastexUAT"
CREDS=$(aws sts assume-role --role-arn "$ROLE_ARN" --role-session-name fix-logs)

export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | jq -r .Credentials.AccessKeyId)
export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | jq -r .Credentials.SecretAccessKey)
export AWS_SESSION_TOKEN=$(echo "$CREDS" | jq -r .Credentials.SessionToken)

echo
echo "does the log group exist:"
echo

aws logs describe-log-groups --log-group-name-prefix "/aws/containerinsights/forecastex-uat/application" --query "logGroups[].logGroupName" --output table

echo
echo "closing script"
echo
