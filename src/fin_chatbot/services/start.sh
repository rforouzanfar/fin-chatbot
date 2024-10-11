#!/bin/sh

# Start the cron daemon
/usr/sbin/cron
echo "Cron daemon started" >> /var/log/cron.log

# Run the scheduled fetch script immediately and in the background
python /fin_chatbot/scheduled_fetch_to_redis.py >> /var/log/scheduled_fetch.log 2>&1 &

# Tail both log files to keep the container running and see output
tail -f /var/log/cron.log /var/log/scheduled_fetch.log