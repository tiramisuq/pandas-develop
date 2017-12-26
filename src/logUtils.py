from BeautifulSoup import BeautifulSoup
import urllib2
import pandas as pd
import numpy as np
import re
from sklearn import preprocessing
import matplotlib.pyplot as plt


def configParser(input, index):
    """
    Parse configuration from html, example:
    <div class="aix-x-DIFF" itemprop="counter name">
        Active EPS bearers|Active IPv6 PDP contexts
    </div>
    aix-x-DIFF
    Active EPS bearers|Active IPv6 PDP contexts.
    :param input: Input file path, "file:///Users/qixiaoxu/PycharmProjects/ml-sklearn/data/config.html".
    :param index: Class name for counter set. Example:
        <SELECT> the columns will be selected,
        <aix-x-DIFF> diff on aix-x.
        Index can be set in config.html accordingly by adding chuck <div class="new-index">,
        with global-unique class name.
    :return: pandas.dataframe.
    """
    print("Parse configuration from %s " % input)
    page = urllib2.urlopen(input)
    soup = BeautifulSoup(page)
    x = soup.body.find('div', attrs={'class': index}).text
    return x


def loadElog(input, selectedColumns=[]):
    """
    Load ELog from input. If selectedColumns is not empty, certain columns will be selected, otherwise all columns are returned.
    :param input: Path of log file.
    :param selectedColumns: List of selected columns.
    :return: pandas.dataframe.
    """
    print("Load log from %s " % input)
    df = pd.read_csv(input, sep="|", engine='python')
    oldcolumn = np.array(df.columns)
    newcolumn = []
    for i in range(len(oldcolumn)):
        if re.match(' Completed', oldcolumn[i]):
            newcolumn.append(oldcolumn[i - 1] + oldcolumn[i])
        else:
            newcolumn.append(oldcolumn[i])
    # for i in range(len(oldcolumn)):
    #     print(newcolumn[i], oldcolumn[i])
    df.columns = newcolumn
    if len(selectedColumns) >= 1:
        return df[selectedColumns]
    else:
        return df


def aggTrans(df, input):
    """
    Action for aix-x-DIFF, aggregate counter parser.
    :param df: Input data frame.
    :param input: List of aggregate counter.
    :return: pandas.dataframe.
    """
    print("Processing Aggregate Counters %s " % input)
    aggout = []
    for i in aggin:
        aggout.append("d" + i)
    for i in range(len(input)):
        df[aggout[i]] = df[input[i]].diff()
    return df


def difffun(df, input):
    """
    Action for aix-y-DIFF, one column minus another relative column.
    :param df: input data frame.
    :param input: List of column pairs, separated by ':' within paired entry.
    :return: pandas.dataframe.
    """
    print("Processing Counters %s " % input)
    for p in input:
        c1 = p.split(":")[0]
        c2 = p.split(":")[1]
        c3 = p
        df[c3] = df[c1] - df[c2]
    return df


def corrDetect(df, inputs, threshold, min):
    """
    Find correlated counters with default threshold=0.8.
    :param df: input data frame.
    :param inputs: List of target columns.
    :param threshold: Defalut value: 0.8, those whose pearson correlation efficient is higher than threshold will be detected.
    :param min: min_periods for pearson correlation efficient.
    :return: pandas.dataframe
    """
    print("Correlation Detection %s %d " % (inputs, min))
    assert threshold > 0.5
    assert min > 0
    dfc = df.corr(method='pearson', min_periods=min)
    print(dfc)
    assert len(inputs) > 0
    for i in inputs:
        dfc = dfc[(dfc[i] > threshold)]
    return dfc[inputs]


configfile = 'file:///Users/qixiaoxu/PycharmProjects/ml-sklearn/data/config.html'
selectColumns = configParser(configfile, "SELECT")
logfile = '/Users/qixiaoxu/PycharmProjects/ml-sklearn/data/head_IMS_pdp.log'
rawdf = loadElog(logfile, selectColumns.split("|"))
# print(rawdf)
aggColumns = configParser(configfile, "aix-x-DIFF")
aggin = aggColumns.split("|")
df1 = aggTrans(rawdf, aggin)
# print(df1)
diffColumns = configParser(configfile, "aix-y-DIFF")
diffin = diffColumns.split("|")
df2 = difffun(rawdf, diffin)
# print(df2)

dfc1 = corrDetect(df2, ['Active PDP contexts', 'Attempted PDP activations Completed'], 0.7, 2)
print(dfc1)
# dfc.plot()
# plt.show()
print("Hello World!")
