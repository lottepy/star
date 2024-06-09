from datetime import datetime
from datetime import timedelta

def time_to_str_iso(time_input):
    if type(time_input) is str:
        time_output = time_input
    else:
        time_output = time_input.strftime('%Y-%m-%d')

    return time_output


def str_to_time_iso(time_input):
    if type(time_input) is str:
        try:
            time_output = datetime.strptime(time_input, '%Y-%m-%d')
        except:
            time_output = datetime.strptime(time_input, '%m/%d/%Y')
    else:
        time_output = time_input

    return time_output


def str_to_date_iso(time_input):
    if type(time_input) is str:
        try:
            time_output = datetime.strptime(time_input, '%Y-%m-%d').date()
        except:
            time_output = datetime.strptime(time_input, '%m/%d/%Y').date()
    else:
        time_output = time_input

    return time_output


def str_list_to_time_iso(time_input_list):
    time_output_list = []
    for time_input in time_input_list:
        if type(time_input) is str:
            time_output = datetime.strptime(time_input, '%Y-%m-%d')
        else:
            time_output = time_input
        time_output_list.append(time_output)

    return time_output_list


def time_list_to_str_iso(time_input_list):
    time_output_list = []
    for time_input in time_input_list:
        if type(time_input) is str:
            time_output = time_input
        else:
            time_output = time_input.strftime('%Y-%m-%d')
        time_output_list.append(time_output)

    return time_output_list


def last_day_of_month(time_input):
    """
    get last day of this month
    :param time_input: datetime
    :return: date
    """
    if time_input.date().month == 12:
        time_output = datetime(time_input.date().year, 12, 31)
    else:
        time_output = datetime(time_input.date().year, time_input.date().month + 1, 1) - timedelta(days=1)

    return time_output

