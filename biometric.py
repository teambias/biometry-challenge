import sqlite3 as sql
import csv
import numpy as np


"""
NOTE: Database created in sqlprompt as following
/*tables match existing csv structure*/
CREATE TABLE questions (
        QuestionId INTEGER, SequenceId INTEGER, QuizDevice INTEGER);
CREATE TABLE test (T INTEGER, X REAL, Y REAL, Z REAL, SequenceId INTEGER);
CREATE TABLE train (T INTEGER, X REAL, Y REAL, Z REAL, Device INTEGER);

/*import data*/
.mode csv
.import questions.csv questions
.import test.csv test
.import train.csv train

/*remove redundant header leftover from csv*/
DELETE FROM questions WHERE QuestionId = 'QuestionId';
DELETE FROM test WHERE T = 'T';
DELETE FROM train WHERE T = 'T';

/*index for speed*/
CREATE INDEX idx_questions_SequenceId ON questions(SequenceId);
CREATE INDEX idx_questions_QuizDevice ON questions(QuizDevice);
CREATE INDEX idx_test_SequenceId ON test(SequenceId);
CREATE INDEX idx_train_Device ON train(Device);
"""


def sqlite3get(dbfile, cmd):
    """
    Run a sqlite command on db and return the result

    Parameters:
    * dbfile - database file
    * cmd - sqlite command

    Returns:
    * resluting tuples OR None if failed
    """
    con = sql.connect(dbfile)
    with con:
        cur = con.cursor()
        cur.execute(cmd)
        dat = cur.fetchall()
        return dat


def extract_csv_summaries(
        in_dbfile="biometric_data.sqlite",
        out_csvfile="biometric_data_summary.csv"):
    """
    Calcualte summary stats for each device and write output as csv.

    Note: slow, call once, then just use the csv
    """
    cmd = """SELECT
        MAX(d)-MIN(d) AS range,
        MIN(d) AS min,
        MAX(d) AS max,
        AVG(d) as avg,
        AVG(d*d)-AVG(d)*AVG(d) AS variance,
        Device
        FROM (
              SELECT t.X*t.X+t.Y*t.Y+t.Z*t.Z AS d, t.Device as Device
              FROM train t) GROUP BY Device"""
    dat = sqlite3get(in_dbfile, cmd)
    csv_writer = csv.writer(open(out_csvfile, "w"))
    csv_writer.writerows(dat)


def make_transformed_variables_command(
        in_csvfile="biometric_data_summary.csv"):
    """
    Make SQL command fragment to transform sql variables so that they are
    normalized to training data.
    """
    dat = csv.reader(open(in_csvfile, "r"))
    keys = ("range", "min", "max", "avg", "variance")
    biodata = []
    for row in dat:
        biodata += [[float(i) for i in row]]
    biodata = np.array(biodata)
    mmin = np.min(biodata, axis=0)
    mmax = np.max(biodata, axis=0)
    cmd = ", \n".join(["({0}-{1})/{2} AS t_{0}".format(
                      x[0], x[1], x[2]) for x in zip(keys, mmin, mmax - mmin)])
    return cmd

#extract_csv_summaries("biometric_data.sqlite", "biometric_data_summary.csv")
cmd = make_transformed_variables_command("biometric_data_summary.csv")
print cmd
