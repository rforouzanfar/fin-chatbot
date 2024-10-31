#!/bin/bash

# Create log directory if it doesn't exist
mkdir -p /var/log

# Create log files if they don't exist
touch /var/log/cron.log
touch /var/log/scheduled_fetch.log
chmod 0644 /var/log/cron.log /var/log/scheduled_fetch.log

# Kill any existing cron processes
pkill cron || true

# Clear any previous log entries
> /var/log/cron.log

# Start cron in foreground mode
/usr/sbin/cron -f &
echo "$(date): Single cron daemon initialized" >> /var/log/cron.log

# Tail logs
exec tail -f /var/log/cron.log /var/log/scheduled_fetch.log