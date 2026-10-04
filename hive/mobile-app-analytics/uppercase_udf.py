#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    fields = line.split('\t')

    user_id = fields[0]
    user_name = fields[1]
    event_type = fields[2]
    device_type = fields[3]
    event_date = fields[4]
    session_duration = fields[5]

    uppercase_name = user_name.upper()

    print(
        user_id + "\t" +
        user_name + "\t" +
        uppercase_name + "\t" +
        event_type + "\t" +
        device_type + "\t" +
        event_date + "\t" +
        session_duration
    )
