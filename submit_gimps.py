import os
import time
import datetime
from math import log2
import requests
import re


gimps_url = "http://v5.mersenne.org/v5server"
computer_id = "COMPUTER_ID"

def send_gimps_request(exponent, sf, ef, assignment_id, msg, partial=True, factor=None):
    params = {
        "px": "GIMPS",
        "v": "0.95",
        "t": "ar",
        "g": computer_id,
        "n": exponent,
        "d": "1",
        "sf": sf,
        "k": assignment_id,
        "m": msg
    }

    if factor:
        # msg = f"M{exponent} has a factor: {factor}"
        result_type = 1
        if partial:
            # not complete range
            ef = round(log2(int(factor)), 3)
            params['f'] = factor
   
    else:
        # msg = f"no factor for M{exponent} from 2^{sf} to 2^{ef}"
        result_type = 4

    params['r'] = result_type
    params['ef'] = ef

    # print(msg)
    resp = requests.get(gimps_url, params=params)
    return resp
 
def parse_assignment_ids(s):
    d = {}
    for line in s.split("\n"):
        line = line.replace("Factor=", "").split(",")
        if len(line) == 4:
            assignment_id = line[0]
            exponent = line[1]
            start_bit = line[2]
            end_bit = line[3]
            d[exponent] = {"assignment_id": assignment_id, "sf": start_bit, "ef": end_bit}

    return d

def current_timestamp():
    return f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}]"

def output_line(s):
    print(f"{current_timestamp()} {s}")

def submit_results(s, d):
    for line in s.split("\n"):
        if line:
            try:
                parsed = re.findall(r"M\d+", line)
                if parsed:
                    exponent = parsed[0].replace("M", "")
                if exponent:
                    assignment_id = d[exponent]['assignment_id']
                if "no factor for M" in line:
                    sf, ef = [i.replace("2^", "") for i in re.findall(r"2\^\d+", line)]
                    output_line(f"no factor {exponent} {sf} {ef} {assignment_id}")
                    resp = send_gimps_request(exponent, sf, ef, assignment_id, line)
                elif "has a factor: " in line:
                    factor = re.findall(r"\s\d+", line)[0].strip()
                    sf, ef = [i.replace(":", "") for i in re.findall(r":\d+", line)]
                    output_line(f"has factor {exponent} {sf} {ef} {assignment_id} {factor}")
                    resp = send_gimps_request(exponent, sf, ef, assignment_id, line, True, factor)
                if resp.status_code != 200:
                    raise Exception(f"failed to submit exponent, status code: {resp.status_code}")
            except Exception as e:
                print(e)

def read_assignment_ids_from_file(assignment_file):
    with open(assignment_file, "r", encoding="utf-8") as f:
        return parse_assignment_ids(f.read())

def process_results(results_path, archive_path, d):
    with open(results_path, "r", encoding="utf-8") as f:
        res = f.read()
        submit_results(res, d)

        with open(archive_path, "a", encoding="utf-8") as g:
            g.write(res)

def get_modification_time(file_path):
    return os.path.getmtime(file_path)

def watch_file(results_path, archive_path, assignment_file, check_interval=30):
    try:
        last_mod_time = get_modification_time(results_path)
        d = read_assignment_ids_from_file(assignment_file)
    except FileNotFoundError:
        last_mod_time = None
        output_line(f"File {file_path} does not exist. Waiting for it to be created...")

    while True:
        time.sleep(check_interval)
        try:
            current_mod_time = get_modification_time(results_path)
            if last_mod_time is None:
                output_line(f"The file {results_path} has been created")
                last_mod_time = current_mod_time
            elif current_mod_time != last_mod_time:
                # output_line(f"The file {results_path} has been modified")
                process_results(results_path, archive_path, d)
                # Clears the file
                open(results_path, "w").close()
                last_mod_time = os.path.getmtime(results_path)
        except FileNotFoundError:
            if last_mod_time is not None:
                output_line(f"The file {results_path} has been deleted")
                last_mod_time = None
            return
        except Exception:
            output_line("Something wrong happened! Exiting...")
            return


if __name__ == "__main__":
    file_to_watch = "mfaktc/results.txt"
    archive_path = "archived_results.txt"
    assignment_file = "assignments.txt"

    output_line(f"Starting to watch file {file_to_watch}")
    output_line(f"Reading assignment file {assignment_file}")
    watch_file(
        results_path=file_to_watch, archive_path=archive_path, assignment_file=assignment_file, check_interval=5)
