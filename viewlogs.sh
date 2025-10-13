

log=/home/ec2-user/pythonQF/log

ls -ltr $log
cat $log/FIXT.1.1-4C001-ForecastEx.messages.current.log | cut -c1-88
tail -15  $log/FIXT.1.1-4C001-ForecastEx.messages.current.log | cut -c1-2288 |  tr '\001' '|' > $log/temp.txt
cat $log/temp.txt | awk '{print $1}' | awk -F"-" '{print $2}'
cat $log/temp.txt | grep Forecast | awk -F"|" '{print $0}'  
cat $log/temp.txt | grep Forecast | awk -F"|" '{print $11}'  

time_str="10:52:14.246678000"

# Split into h m s ns
IFS=':.' read -r h m s ns <<< "$time_str"

# Force base-10 to avoid octal issues with leading zeros
total_ns=$(( (10#$h*3600 + 10#$m*60 + 10#$s)*1000000000 + 10#$ns ))
echo "$total_ns"                          # e.g., 39134246678000  (nanoseconds since midnight)

# If you also want seconds as a floating number:
awk -v n="$total_ns" 'BEGIN{printf "%.9f\n", n/1e9}'   # e.g., 39134.246678000

cat $log/temp.txt | awk '{print $1}' | awk -F"-" '{print $2}' |\
awk -F '[:.]' '
BEGIN {prevval = 0}
{
val=(($1*3600 + $2*60 + $3) + $4/1e9)
diff=val-prevval
prevval=val
printf "%.9f %.9f\n", val,diff }'
