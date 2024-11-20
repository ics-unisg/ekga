"""A simple GUI for labeling ground truth data."""
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Union, Tuple
import json
import streamlit as st
import pandas as pd

st.set_page_config(page_title="GT-Label-GUI", layout="wide", page_icon="📌")


@dataclass
class Activity:
    """ Represents an activity instance. """
    name: str
    active: bool = False
    current_execution: Tuple[Union[None, datetime], Union[None, datetime]] = (None, None)
    properties: dict = None
    id: uuid.UUID = None

    def __post_init__(self):
        self.id = uuid.uuid4()


MOCK_ACTIVITIES = [
    Activity("Donor check-in",
             properties={"resource": "Nurse1", "patient": "John Doe"}),
    Activity("Perform Hand hygiene",
             properties={"resource": "Nurse1", "patient": "John Doe"}),
    Activity("Check blood drawing machine",
             properties={"resource": "Nurse1", "patient": "John Doe"}),
    Activity("Apply tourniquet",
             properties={"resource": "Nurse2", "patient": "John Doe"}),
    Activity("Perform Hand hygiene",
             properties={"resource": "Nurse2", "patient": "John Doe"}),
    Activity("Disinfect injection site",
             properties={"resource": "Nurse2", "patient": "John Doe"}),
    Activity("Draw blood", properties={"patient": "John Doe"}),
]

if 'activities' not in st.session_state:
    st.session_state.activities = []

if 'recorded_executions' not in st.session_state:
    st.session_state.recorded_executions = []

if "tracking_active" not in st.session_state:
    st.session_state.tracking_active = False


def start_stop_tracking():
    """ Start or stop tracking of activity executions."""
    st.session_state.tracking_active = not st.session_state.tracking_active
    st.session_state.recorded_executions = []


def click_recording_button(activity: Activity):
    """ Start or stop recording of an activity execution. """
    activity.active = not activity.active


def add_activity(activity: Activity):
    """ Add activity (not execution) to the list, making sure there is no duplicate beforehand

    Duplicate means the same name and the same properties
    """
    for act in st.session_state.activities:
        if act.name == activity.name and act.properties == activity.properties:
            st.toast("An activity that has been added already exists, so it was not added again. "
                     "If you want to add it again, please change the name or the properties. "
                     "Activity: " + activity.name + ", with properties: " + str(
                activity.properties))
            return
    st.session_state.activities.append(activity)


def add_activity_manually(activity: Activity, prop_success: bool):
    """ Add activity (not execution) to the list, making sure there is no duplicate beforehand

    Duplicate means the same name and the same properties
    """
    if not prop_success:
        st.toast("Invalid JSON format, activity was not added")
        return
    add_activity(activity)


def add_activities_from_file(file):
    """ Add activities from a CSV file.

    The CSV file should have a column named 'name' and other columns are
    interpreted as properties."""
    df = pd.read_csv(file)
    # check if there is a column named name, else raise an error
    if "name" not in df.columns:
        raise ValueError("No column named 'name' found in the uploaded CSV file")
    for _, row in df.iterrows():
        props = {key.strip(): row[key].strip() for key in df.columns if
                 not pd.isna(row[key]) and key != "name"}
        add_activity(Activity(
            name=row["name"],
            properties=props)
        )


@st.cache_data
def convert_to_csv(executions: List[Dict[str, Union[str, int]]], activities):
    """ Convert the recorded executions to a CSV string."""
    csv_content = "activity_id,activity_name,start,end"
    if len(property_keys(activities)) > 0:
        csv_content += ","
    # add resources as columns
    csv_content += ",".join(list(property_keys(activities)))
    csv_content += "\n"
    for elem in executions:
        for activity_id, (start, end) in elem.items():
            csv_content += (f"{activity_id},"
                            f"{get_activity_by_id(activities, activity_id).name},"
                            f"{start},"
                            f"{end}")
            for key in property_keys(activities):
                if key in get_activity_by_id(activities, activity_id).properties.keys():
                    csv_content += f",{get_activity_by_id(activities, activity_id).properties[key]}"
                else:
                    csv_content += ","
            csv_content += "\n"
    return csv_content


@st.cache_data
def property_keys(activities):
    """ Return all keys of the properties of the activities.  """
    return {key for activity in activities for key in activity.properties.keys()}


@st.cache_data
def property_values(activities, key: str):
    """ Return all values of the properties of the activities for a given key. """
    val_set = {activity.properties[key] for activity in activities if
               key in activity.properties.keys()}
    val_list = list(val_set)
    val_list.sort()
    return val_list


@st.cache_resource
def activites_with_property(activities, key: str, value: str):
    """ Return all activities with a given key-value pair in the properties. """
    filtered_acts = [activity for activity in activities if
                     key in activity.properties.keys() and activity.properties[key] == value]
    return filtered_acts


@st.cache_resource
def activities_without_key(activities, key: str):
    """ Return all activities without a given key in the properties. """
    return [activity for activity in activities if key not in activity.properties.keys()]


def get_activity_by_id(activities, act_id):
    """ Get an activity by its ID. """
    rel_act = [activity for activity in activities if activity.id == act_id]
    if len(rel_act) > 1:
        raise ValueError("More than one activity with the same ID")
    if len(rel_act) == 0:
        return None
    return rel_act[0]


###########################################################
def setup_mqtt():

    import paho.mqtt.client as mqtt

    broker = "ftsim.weber.ics.unisg.ch"
    port = 1883

    client = mqtt.Client()
    client.username_pw_set("ftsim", "unisg")
    client.connect(broker, port)

    return client

def exit_mqtt(client):
    client.disconnect()
###########################################################


col1, col2, col3, col4 = st.columns(4)
with col1:
    if not st.session_state.tracking_active:
        with st.popover("Add activities"):
            st.subheader("Add activities manually")
            act_name = st.text_input("Activity name")
            activity_properties = st.text_input("Properties (JSON format)", value="{}")
            try:
                properties = json.loads(activity_properties)
                property_success = True # pylint: disable=invalid-name
            except json.JSONDecodeError:
                properties = {}
                property_success = False # pylint: disable=invalid-name
            st.button("Add",
                      key="add_activity",
                      on_click=lambda: add_activity_manually(
                          Activity(name=act_name,
                                   properties=properties), property_success),
                      )
            st.subheader("Upload activities")
            f = st.file_uploader("Upload activities",
                                 type="csv",
                                 help="The CSV file should have a column named 'name' and other "
                                      "columns are interpreted as properties.")
            if f is not None:
                st.button("Add",
                          on_click=lambda: add_activities_from_file(f))
            st.subheader("Add mock activities")
            if st.button("Add mock activities", key="add_mock_activities"):
                st.session_state.activities.extend(MOCK_ACTIVITIES)
                st.rerun()
    else:
        csv = convert_to_csv(st.session_state.recorded_executions,
                             st.session_state.activities)
        st.download_button(
            label="Download as CSV",
            key="download_csv_bt",
            data=csv,
            file_name="gt.csv",
            mime="text/csv",
        )

with col2:
    num_cols = st.number_input(
        "Number of columns",
        key="num_cols_input",
        min_value=1,
        max_value=8,
        value=3)
with col3:
    prop_keys = property_keys(st.session_state.activities)
    split_view = st.selectbox("Split view by", prop_keys, index=None)
with col4:
    st.toggle("Tracking mode",
              key="sst",
              on_change=start_stop_tracking,
              help="When active, activity executions can be tracked below. "
                   "Switching between tracking mode and edit mode resets recorded executions.")

st.divider()


@st.experimental_fragment
def activity_recorder(activity: Activity, hide_properties: Union[None, List[str]] = None):
    """ Display an activity "box" with a button to start/stop recording/delete. """
    with st.container(border=True):
        color_dot = "🔴" if not activity.active else "🟢"
        color_dot = color_dot if st.session_state.tracking_active else "🔵"
        st.markdown(f"**{color_dot} {activity.name}**")
        for key, value in activity.properties.items():
            # key in grey, value in black
            if hide_properties is not None and key in hide_properties:
                continue
            st.markdown(f":gray[{key}]: {value}")
        if st.session_state.tracking_active:
            if st.button("Start/Stop",
                         key=str(activity.id) + "_startstop",
                         on_click=lambda: click_recording_button(activity),
                         use_container_width=True):
                if activity.active:
                    activity.current_execution = (datetime.now(), None)
                else:
                    activity.current_execution = (activity.current_execution[0], datetime.now())
                    st.session_state.recorded_executions.append(
                        {activity.id: activity.current_execution})
                    ###########################################################
                    CONN = setup_mqtt()
                    mqtt_activity = get_activity_by_id(st.session_state.activities, activity.id)
                    CONN.publish("smart-healthcare/DEMO", json.dumps([mqtt_activity.name, mqtt_activity.properties]))
                    exit_mqtt(CONN)
                    ###########################################################
                    activity.current_execution = (None, None)
                    st.rerun()
        else:
            if st.button("Delete",
                         key=activity.name + str(activity.properties) + "delete",
                         on_click=lambda: st.session_state.activities.remove(activity),
                         use_container_width=True):
                st.rerun()


if not split_view:
    cols = st.columns(num_cols)
    for i in range(0, len(st.session_state.activities), num_cols):
        for j in range(num_cols):
            if i + j < len(st.session_state.activities):
                with cols[j]:
                    activity_recorder(st.session_state.activities[i + j])
else:
    prop_vals = property_values(st.session_state.activities,
                                split_view)
    number_distinct_values = len(prop_vals)
    act_wo_key = activities_without_key(st.session_state.activities,
                                        split_view)
    if len(act_wo_key) > 0:
        number_distinct_values += 1
        prop_vals.append("No " + split_view)
    cols_outer = st.columns(number_distinct_values)
    for i, val in enumerate(prop_vals):
        with cols_outer[i]:
            col_outer_container = st.container(border=True)
            col_outer_container.subheader(val)
            if ((len(act_wo_key) > 0 and i < len(prop_vals) - 1)
                    or len(act_wo_key) == 0):
                act_w_prop_val = activites_with_property(st.session_state.activities,
                                                         split_view,
                                                         val)
            else:
                act_w_prop_val = act_wo_key
            for k in range(0, len(act_w_prop_val), num_cols):
                cols_inner = st.columns(num_cols)
                for j in range(num_cols):
                    if k + j < len(act_w_prop_val):
                        with cols_inner[j]:
                            activity_recorder(act_w_prop_val[k + j], hide_properties=[split_view])
