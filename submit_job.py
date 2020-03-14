from __future__ import print_function
import sys
from datetime import date, timedelta
from datetime import datetime as dt
import subprocess

if len(sys.argv) < 4:
    print('need processing date string: python {} start_date end_date format like YYYY-mm-dd and python_file'.format(sys.argv[0]))
    exit(1)
start_date_str = sys.argv[1]
end_date_str = sys.argv[2]
python_file = sys.argv[3]

print("*" * 40)
print("start_date_str", start_date_str)
print("end_date_str", end_date_str)
print("python_file", python_file)
print("*" * 40)

start_date = dt.strptime(start_date_str, "%Y-%m-%d")
end_date = dt.strptime(end_date_str, "%Y-%m-%d")
day_count = (end_date - start_date).days
for curdate in (start_date + timedelta(n) for n in range(day_count)):
    curdate_str = dt.strftime(curdate, "%Y-%m-%d")
    args = "/apps/anaconda2/bin/python {} {}".format(python_file, curdate_str)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    print(s_err)
