import pandas as pd
import os
from datetime import datetime

cols = ["trace", "name", "timestamp", "location"]
all_df_collected = pd.DataFrame(columns=cols)

for csv_file in sorted(os.listdir("DEMO\collected_data")):
    full_path = os.path.join("DEMO\collected_data", csv_file)
    df_collected = pd.read_csv(full_path)
    df_collected.insert(0, "identifier", csv_file.split("_")[-1].split(".")[0])
    df_collected = df_collected[["identifier", "activity_name", "end", "patient"]]
    df_collected["end"] = pd.to_datetime(df_collected["end"])
    df_collected["end"] = df_collected["end"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00").str.slice(0, -9) + "+00:00"
    df_collected.columns = cols
    all_df_collected = all_df_collected.append(df_collected, ignore_index=True)

all_df_collected


log_template = """<?xml version="1.0" encoding="utf-8" ?>
<log xmlns="http://www.xes-standard.org/" xes.version="2.0" xes.features="nested-attributes">
    ...
    <trace>
    </trace>
</log>"""

event_template = """        <event>
            <string key="concept:name" value=""/>
            <date key="time:timestamp" value=""/>
            <string key=":" value=""/>
        </event>
"""

map_activities = {
    # HCW ambiguous
    "HCW check-in": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "HCW check-out": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    # HCW performs unambiguous
    "Check blood drawing machine": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Apply tourniquet": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Disinfect injection site": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Insert needle": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Remove tourniquet": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Start blood drawing machine": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Monitor patient": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Stop blood drawing machine": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Remove needle": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    "Take out samples": [("perform", "hcw", "PLACEHOLDER_HCW"), ("at", "location", "LOOKUP_LOCATION")],
    # Hand hygiene ambiguous
    "Hand hygiene": [("perform", "hcw", "PLACEHOLDER_HCW")],
    "Perform Hand hygiene": [("perform", "hcw", "PLACEHOLDER_HCW")],
    "Hand hygiene and wear gloves": [("perform", "hcw", "PLACEHOLDER_HCW")],
    "Remove gloves and hand hygiene": [("perform", "hcw", "PLACEHOLDER_HCW")],
    "Remove gloves, dispose of gloves, and hand hygiene": [("perform", "hcw", "PLACEHOLDER_HCW")],
    # Donor performs unambiguous
    "Donor check-in": [("perform", "donor", "PLACEHOLDER_DONOR"), ("at", "location", "LOOKUP_LOCATION")],
    "Donor check-out": [("perform", "donor", "PLACEHOLDER_DONOR"), ("at", "location", "LOOKUP_LOCATION")],
    # Disturbance
    "Disturbance": [("PLACEHOLDER_DISTURBANCE", "PLACEHOLDER_DISTURBANCE", "PLACEHOLDER_DISTURBANCE"), ("at", "location", "LOOKUP_LOCATION")]
}

for i,row in list(all_df_collected.iterrows())[::-1]:

    add_event = event_template[:62]
    add_event += f"{row['name']}"+ event_template[62:112]
    add_event += f"{row['timestamp']}" + event_template[112:141]
    get_activity = map_activities[row['name']]
    add_event += get_activity[0][0] + event_template[141:142]
    add_event += get_activity[0][1]  + event_template[142:151]
    add_event += get_activity[0][2]  + event_template[151:154]
    if len(get_activity) == 2:
        add_event += f'''\n            <string key="{get_activity[1][0]}:{get_activity[1][1]}" value="{row['location']}"/>'''
    add_event += event_template[154:]

    log_template = log_template[:154] + add_event + log_template[154:]

log_template


with open("full_log.txt", "w") as f:
    f.write(log_template)
