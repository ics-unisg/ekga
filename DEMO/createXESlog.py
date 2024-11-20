import pandas as pd
import os
from datetime import datetime

cols = ["case:concept:name", "concept:activity_id", "concept:activity_name", "time:startTimestamp", "time:endTimestamp", "concept:patient"]
all_df_collected = pd.DataFrame(columns=cols)

for csv_file in sorted(os.listdir("DEMO\collected_data")):
    full_path = os.path.join("DEMO\collected_data", csv_file)
    df_collected = pd.read_csv(full_path)
    df_collected.insert(0, "identifier", csv_file.split("_")[-1].split(".")[0])
    df_collected["start"] = pd.to_datetime(df_collected["start"])
    df_collected["end"] = pd.to_datetime(df_collected["end"])
    df_collected.columns = cols
    all_df_collected = all_df_collected.append(df_collected, ignore_index=True)

log_template = """<?xml version="1.0" encoding="utf-8" ?>
<log xmlns="http://www.xes-standard.org/" xes.version="2.0" xes.features="nested-attributes">
    <string key="creator" value="cpee.org"/>
    <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>
    <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>
    <extension name="ID" prefix="id" uri="http://www.xes-standard.org/identity.xesext"/>
    <extension name="Lifecycle" prefix="lifecycle" uri="http://www.xes-standard.org/lifecycle.xesext"/>
    <extension name="CPEE" prefix="cpee" uri="http://cpee.org/cpee.xesext"/>
    <extension name="stream" prefix="stream" uri="https://cpee.org/datastream/datastream.xesext"/>
    <global scope="trace">
        <string key="concept:name" value="__NOTSPECIFIED__"/>
    </global>
</log>"""

point_template = """
                <list key="stream:point">
                    <string key="stream:id" value=/>
                    <string key="stream:source" value=/>
                    <string key="stream:value" value=/>
                    <date key="stream:timestamp" value=/>
                </list>
                <list key="stream:point">
                    <string key="stream:id" value=/>
                    <string key="stream:source" value=/>
                    <string key="stream:value" value=/>
                    <date key="stream:timestamp" value=/>
                </list>"""

current_collection = None
for i,row in list(all_df_collected.iterrows())[::-1]:
    if current_collection != row["case:concept:name"]:
        if current_collection:
            if current_collection != "s1s1":
                log_template = log_template[:838] + add_trace + "\n" + log_template[838:]
    if current_collection != row["case:concept:name"]:
        add_trace = """    <trace>
        <string key="concept:name" value=/>
        <list key="stream:datacontext">
            <list key="stream:datastream">
            </list>
        </list>
    </trace>"""
        add_trace = add_trace[:53] + '"' + row["case:concept:name"] + '"' + add_trace[53:]
        add_trace
        current_collection =  row["case:concept:name"]

    add_points = point_template[:93]
    add_points += f'''"{row['concept:activity_name']}"''' + point_template[93:150]
    add_points += f'''"{row['concept:patient']}"''' + point_template[150:206]
    add_points += '"Start"' + point_template[206:264]
    add_points += f'''"{row['time:startTimestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+01:00'}"''' + point_template[264:383]
    add_points += f'''"{row['concept:activity_name']}"''' + point_template[383:440]
    add_points += f'''"{row['concept:patient']}"''' + point_template[440:496]
    add_points += '"End"' + point_template[496:554]
    add_points += f'''"{row['time:endTimestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+01:00'}"''' + point_template[554:]

    add_trace = add_trace[:144] + add_points + add_trace[144:]

    if current_collection == "s1s1":
        if i == 0:
            log_template = log_template[:838] + add_trace + "\n" + log_template[838:]

with open("full_log_extendedXES.txt", "w") as f:
    f.write(log_template)
