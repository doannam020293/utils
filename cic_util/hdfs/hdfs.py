import subprocess
from datetime import datetime
from datetime import datetime as dt
import numpy as np


def start_date_path(path):
    """
    return start date of path
    :param path:
    :return:
    """
    all_files = get_dirs(path)
    all_dates = [dt.strptime(f.split("=")[1], "%Y-%m-%d").date()
                 for f in all_files if "__HIVE_DEFAULT_PARTITION__" not in f]
    return np.min(all_dates)


def end_date_path(path):
    """
    return start date of path
    :param path:
    :return:
    """
    all_files = get_dirs(path)
    all_dates = [dt.strptime(f.split("=")[1], "%Y-%m-%d").date()
                 for f in all_files if "__HIVE_DEFAULT_PARTITION__" not in f]
    return np.max(all_dates)

def get_dirs(dir_in):
    args = "hdfs dfs -ls "+ dir_in + "| awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, _ = proc.communicate()
    all_dart_dirs = s_output.split()
    return all_dart_dirs

def check_dir_exist(dir_path):
    args = ['hadoop', 'fs', '-test', '-d', dir_path]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode == 0

def delete_file(filename):
    args = ['hdfs', 'dfs', '-rm', '-r', filename]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    print(stdout, stderr)
    return stdout

def check_file_exist(file_path):
    args = ['hadoop', 'fs', '-test', '-f', file_path]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode == 0

def compress_csv(spark, file_path, bz_dir_path):
    new_file_path = "{}/{}.bz2".format(bz_dir_path, file_path.split("/")[-1])
    spark.read.csv(file_path).write.option("compression","bzip2").csv(new_file_path)

def move_to_backup(file_path, dir_path):
    args = "hdfs dfs -mv {} {}" .format(file_path, dir_path)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.communicate()
    return proc.returncode == 0

def mkdir_hdfs(dir_path):
    args = ['hdfs', 'dfs', '-mkdir', dir_path]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode == 0

def last_date(partitioned_dir):
    all_files = get_dirs(partitioned_dir)
    all_dates = [datetime.strptime(f.split("=")[1], "%Y-%m-%d").date() for f in all_files]
    return __builtins__.max(all_dates)

def get_size(path):
    args = "hdfs dfs -du " + path
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    stats = dict()
    for line in s_output.split('\n'):
        if "date" not in line:
            continue
        date = line.split("=")[-1]
        size = int(line.split()[0])
        stats[date] = size
    if "__HIVE_DEFAULT_PARTITION__" in stats:
        del stats['__HIVE_DEFAULT_PARTITION__']
    return stats


def set_rep_hdfs(path, rep =1):
    args = "hdfs dfs -setrep -R {} {}".format(rep, path)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    print(s_err)
    return proc.returncode == 0

def write_dataframe_to_parquet(df, output_file):
    df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(output_file)


def rename_file(file_path, new_file_path):
    args = ['sudo', '-u', 'hdfs', 'hadoop', 'dfs', '-mv', file_path, new_file_path]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode == 0
