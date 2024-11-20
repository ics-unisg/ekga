import pytest
from streamlit.testing.v1 import AppTest


def test_start_page():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    # general checks
    assert len(at.number_input) == 1
    assert len(at.selectbox) == 1
    assert len(add_mock_buttons) == 1
    assert len(at.toggle) == 1


def test_add_activities():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    # checks before adding activities
    assert len(at.selectbox) == 1
    with pytest.raises(IndexError):
        at.selectbox[0].select_index(0)
    assert len([md.value for md in at.markdown if "Apply tourniquet" in md.value]) == 0
    assert len(at.session_state.activities) == 0
    add_mock_buttons[0].click().run()
    # checks after adding activities
    assert len([md.value for md in at.markdown if "Apply tourniquet" in md.value]) == 1
    assert len(at.session_state.activities) == 7
    assert len([del_button for del_button in at.button if del_button.label == "Delete"]) > 0
    [del_button for del_button in at.button if del_button.label == "Delete"][0].click().run()
    assert len(at.session_state.activities) == 6


def test_split_view():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    add_mock_buttons[0].click().run()
    # checks for split view
    assert "Nurse1" not in [sh.value for sh in at.subheader]
    assert "Nurse2" not in [sh.value for sh in at.subheader]
    assert "No resource" not in [sh.value for sh in at.subheader]
    at.selectbox[0].select_index(0)
    at.selectbox[0].select_index(1)
    with pytest.raises(IndexError):
        at.selectbox[0].select_index(2)
    at.selectbox[0].select_index(None)
    at.selectbox[0].set_value(None).run()
    at.selectbox[0].set_value("resource").run()
    assert "Nurse1" in [sh.value for sh in at.subheader]
    assert "Nurse2" in [sh.value for sh in at.subheader]
    assert "No resource" in [sh.value for sh in at.subheader]
    at.selectbox[0].select_index(None).run()


def test_check_column_choice():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    add_mock_buttons[0].click().run()
    # checks for number of columns
    assert len(at.columns) == 7
    num_col_in = [inp for inp in at.number_input if inp.key == "num_cols_input"][0]
    num_col_in.set_value(5).run()
    assert len(at.columns) == 9
    num_col_in.set_value(3).run()


def test_tacking_mode():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    add_mock_buttons[0].click().run()
    # check for toggle tracking mode
    tracking_toggle = [tog for tog in at.toggle if tog.key == "sst"][0]
    assert tracking_toggle.value is False
    assert len([del_button for del_button in at.button if del_button.label == "Delete"]) > 0
    tracking_toggle.set_value(True).run()
    assert len([del_button for del_button in at.button if del_button.label == "Delete"]) == 0


def test_recording():
    at = AppTest.from_file("gt_label_gui.py").run()
    add_mock_buttons = [button for button in at.button if button.key == "add_mock_activities"]
    add_mock_buttons[0].click().run()
    [tog for tog in at.toggle if tog.key == "sst"][0].set_value(True).run()
    assert len(at.session_state.recorded_executions) == 0
    start_stop_buttons = [ssb for ssb in at.button if ssb.label == "Start/Stop"]
    start_stop_buttons[0].click().run()
    start_stop_buttons[0].click().run()
    assert len(at.session_state.recorded_executions) == 1
    [tog for tog in at.toggle if tog.key == "sst"][0].set_value(False).run()
    assert len(at.session_state.recorded_executions) == 0
