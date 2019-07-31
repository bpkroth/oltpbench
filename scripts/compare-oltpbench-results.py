#!/usr/bin/env python
# compare-oltpbench-results.py
# 2019-07-29
# bpkroth
#
# A simple script to compare two oltpbench run results.

import numpy as np
import os.path
import sys
from os.path import isfile
from os.path import basename
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--mode', dest='mode')
parser.add_argument('--resultsFile1', dest='resultsFile1', required=True)
parser.add_argument('--resultsFile2', dest='resultsFile2', required=True)

def usage(msg):
    if msg:
        print >> sys.stderr, "ERROR: ", msg, "\n"
    parser.print_help()
    sys.exit(1)

def processArgs():
    args = parser.parse_args()

    if not args.resultsFile1:
        usage('Missing argument --resultsFile1')
    if not args.resultsFile2:
        usage('Missing argument --resultsFile2')

    if not isfile(args.resultsFile1):
        usage('--resultsFile1 "{0}" is not accessible'.format(args.resultsFile1))
    if not isfile(args.resultsFile2):
        usage('--resultsFile2 "{0}" is not accessible'.format(args.resultsFile2))

    # Open the files.
    resultsFile1 = open(args.resultsFile1, 'r')
    if not resultsFile1:
        usage('--resultsFile1 "{0}" is not readable'.format(args.resultsFile1))

    resultsFile2 = open(args.resultsFile2, 'r')
    if not resultsFile2:
        usage('--resultsFile2 "{0}" is not readable'.format(args.resultsFile2))

    # Verify that the formats of both files match.
    resultsFile1HeaderLine = resultsFile1.readline()
    resultsFile2HeaderLine = resultsFile2.readline()
    if resultsFile1HeaderLine != resultsFile2HeaderLine:
        usage('Header line format for --resultsFile1 "{0}" and --resultsFile2 "{1}" do not match.'.format(
            args.resultsFile1, args.resultsFile2))

    aggregateResultsFileHeaderLine = 'time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms), tp (req/s) scaled'
    rawResultsFileHeaderLine = 'Transaction Type Index,Transaction Name,Start Time (microseconds),Latency (microseconds),Worker Id (start number),Phase Id (index in config file)'

    if not args.mode:
        # try to auto detect
        if resultsFile1HeaderLine.rstrip() == aggregateResultsFileHeaderLine:
            args.mode = 'aggregate'
        elif resultsFile1HeaderLine.rstrip() == rawResultsFileHeaderLine:
            args.mode = 'raw'

    mode = 'undefined'
    if args.mode == 'aggregate' or args.mode == 'res':
        mode = 'aggregate'
    elif args.mode == 'raw' or args.mode == 'csv':
        mode = 'raw'
    else:
        usage('Unhandled mode: "{0}"'.format(args.mode))

    # Make sure that our declared mode matches the headers in the files.
    if mode == 'aggregate':
        if resultsFile1HeaderLine.rstrip() != aggregateResultsFileHeaderLine:
            usage('Headerline format for results files does not match mode "{0}" expectation: "{1}"'.format(mode, resultsFile1HeaderLine))
    elif mode == 'raw':
        if resultsFile1HeaderLine.rstrip() != rawResultsFileHeaderLine:
            usage('Headerline format for results files does not match mode "{0}" expectation: "{1}"'.format(mode, resultsFile1HeaderLine))
    else:
        raise Exception('Unhandled mode: "{0}"'.format(mode))

    # Verify that the step sizes make sense to compare.
    if mode == 'aggregate':

        resultsFile1FirstResultsLine = resultsFile1.readline()
        resultsFile2FirstResultsLine = resultsFile2.readline()

        resultsFile1SecondResultsLine = resultsFile1.readline()
        resultsFile2SecondResultsLine = resultsFile2.readline()

        resultsFile1StepSize = resultsFile1SecondResultsLine.split(',')[0]
        resultsFile2StepSize = resultsFile2SecondResultsLine.split(',')[0]

        if resultsFile1StepSize != resultsFile2StepSize:
            usage('Step size for --resultsFile1 "{0}" ({1}) and --resultsFile2 "{2}" ({3}) do not match.'.format(
                args.resultsFile1, resultsFile1StepSize, args.resultsFile2, resultsFile2StepSize))

        # This may not make sense to do if the result runs are so far off that
        # they take longer or complete different amounts.
        '''
        # This is somewhat ineffecient, but allows us to easily use zip() to
        # iterate over both files later on
        # First move back to the begining.
        resultsFile1.seek(0)
        resultsFile2.seek(0)
        resultsFile1LineCount = len(resultsFile1.readlines())
        resultsFile2LineCount = len(resultsFile2.readlines())
        if resultsFile1LineCount != resultsFile2LineCount:
            usage('Line count for --resultsFile1 "{0}" ({1}) does not match line count for --resultsFile2 "{2}" ({3})'.format(
                args.resultsFile1, resultsFile1LineCount, args.resultsFile2, resultsFile2LineCount))

        '''

    # Rewind the files (again).
    resultsFile1.seek(0)
    resultsFile2.seek(0)

    return mode, resultsFile1, resultsFile2

# Example output.res:
# time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms), tp (req/s) scaled
# 0,38.000,1023.614,435.037,1015.377,1027.236,1048.301,1062.389,1092.329,2943.103,2946.521,0.001
# ...
def analyzeAggregateResultsFile(resultsFile):
    resultsFileResults = {
            'throughput': list(),
            'avg_lat': list(),
            'median_lat': list(),
            'p90_lat': list(),
            'p99_lat': list(),
            'max_lat': list(),
    }

    for resultsLine in resultsFile:
        if not resultsLine[0].isdigit():
            continue
        # else ...
        resultsLineFields = resultsLine.split(',')
        resultsFileResults['throughput'].append(float(resultsLineFields[1]))
        resultsFileResults['avg_lat'].append(float(resultsLineFields[2]))
        resultsFileResults['median_lat'].append(float(resultsLineFields[5]))
        resultsFileResults['p90_lat'].append(float(resultsLineFields[7]))
        resultsFileResults['p99_lat'].append(float(resultsLineFields[9]))
        resultsFileResults['max_lat'].append(float(resultsLineFields[10]))

    resultsFileAnalysis = dict()
    for metric in resultsFileResults.keys():
        resultsFileAnalysis[metric] = {
            'avg': np.mean(resultsFileResults[metric]),
            'stddev': np.std(resultsFileResults[metric]),
            'median': np.median(resultsFileResults[metric]),
            'p90': np.percentile(resultsFileResults[metric], 90),
            'p99': np.percentile(resultsFileResults[metric], 99),
            'max': np.max(resultsFileResults[metric]),
        }

    return resultsFileAnalysis

def printAggregateResultsFileAnalysisPart(resultsFile, resultsFileAnalysis, metric):
    print 'ResultsFile {0:55s}:  {1:12s}: Avg: {2:12.3f}, StdDev: {3:12.3f}, Median: {4:12.3f}, P90: {5:12.3f}, P99: {6:12.3f}, Max: {7:12.3f}'.format(
        basename(resultsFile.name),
        metric,
        resultsFileAnalysis[metric]['avg'],
        resultsFileAnalysis[metric]['stddev'],
        resultsFileAnalysis[metric]['median'],
        resultsFileAnalysis[metric]['p90'],
        resultsFileAnalysis[metric]['p99'],
        resultsFileAnalysis[metric]['max'])

def compareAggregateResultsFiles(resultsFile1, resultsFile2):
    resultsFile1Analysis = analyzeAggregateResultsFile(resultsFile1)
    resultsFile2Analysis = analyzeAggregateResultsFile(resultsFile2)

    for metric in ['throughput', 'avg_lat', 'median_lat', 'p90_lat', 'p99_lat', 'max_lat']:
        printAggregateResultsFileAnalysisPart(resultsFile1, resultsFile1Analysis, metric)
        printAggregateResultsFileAnalysisPart(resultsFile2, resultsFile2Analysis, metric)

        resultsComparisonsDiff = dict()
        resultsComparisonsPct = dict()
        for agg in resultsFile1Analysis[metric].keys():
            resultsComparisonsDiff[agg] = float(resultsFile2Analysis[metric][agg] - resultsFile1Analysis[metric][agg])
            resultsComparisonsPct[agg] = float(resultsComparisonsDiff[agg] / resultsFile1Analysis[metric][agg] * 100)

        print 'ResultsFile1 vs ResultsFile2 {0:38s}:  {1:12s}: Avg: {2:+12.3f}, StdDev: {3:+12.3f}, Median: {4:+12.3f}, P90: {5:+12.3f}, P95: {6:+12.3f}, Max: {7:+12.3f}'.format(
            '', # padding
            metric,
            resultsComparisonsDiff['avg'],
            resultsComparisonsDiff['stddev'],
            resultsComparisonsDiff['median'],
            resultsComparisonsDiff['p90'],
            resultsComparisonsDiff['p99'],
            resultsComparisonsDiff['max'])

        print 'ResultsFile1 vs ResultsFile2 (%) {0:34s}:  {1:12s}: Avg: {2:+11.3f}%, StdDev: {3:+11.3f}%, Median: {4:+11.3f}%, P90: {5:+11.3f}%, P99: {6:+11.3f}%, Max: {7:+11.3f}%'.format(
            '', # padding
            metric,
            resultsComparisonsPct['avg'],
            resultsComparisonsPct['stddev'],
            resultsComparisonsPct['median'],
            resultsComparisonsPct['p90'],
            resultsComparisonsPct['p99'],
            resultsComparisonsPct['max'])

        print

# Example output.csv:
# NOTE: "Start Time" is not actually in microseconds!
# Transaction Type Index,Transaction Name,Start Time (microseconds),Latency (microseconds),Worker Id (start number),Phase Id (index in config file)
# 4,Delivery,1563834709.181374,744571,0,0
# ...
def analyzeRawResultsFile(resultsFile):
    microsecondsPerSecond = 1000000
    resultsFileLatencies = list()
    firstStartTime = sys.float_info.max
    lastEndTime = 0
    for resultsLine in resultsFile:
        if not resultsLine[0].isdigit():
            continue
        # else ...
        resultsLineFields = resultsLine.split(',')
        resultsLineLatency = int(resultsLineFields[3])
        resultsLineStartTime = float(resultsLineFields[2])
        resultsLineEndTime = resultsLineStartTime + (resultsLineLatency / microsecondsPerSecond)
        resultsFileLatencies.append(resultsLineLatency)
        firstStartTime = min(firstStartTime, resultsLineStartTime)
        lastEndTime = max(lastEndTime, resultsLineEndTime)

    totalTimeSecs = lastEndTime - firstStartTime
    totalTransactions = len(resultsFileLatencies)

    resultsFileAnalysis = {
        'totalTime': totalTimeSecs,
        'totalTransactions': totalTransactions,
        'throughput': totalTransactions / totalTimeSecs,
        'avg': np.mean(resultsFileLatencies),
        'stddev': np.std(resultsFileLatencies),
        'median': np.median(resultsFileLatencies),
        'p90': np.percentile(resultsFileLatencies, 90),
        'p99': np.percentile(resultsFileLatencies, 99),
        'max': np.max(resultsFileLatencies),
    }

    return resultsFileAnalysis

def printRawResultsFileAnalysis(resultsFile, resultsFileAnalysis):
    print 'ResultsFile {0:55s}: Transactions: {1:12d}, Time (s): {2: 12.3f}, Throughput (txns/sec): {3:12.3f},  Latencies (us):  Average: {4:12.3f}, StdDev: {5:12.3f}, Median: {6:12.3f}, P90: {7:12.3f}, P99: {8:12.3f}, Max: {9:12.3f}'.format(
            basename(resultsFile.name),
            resultsFileAnalysis['totalTransactions'],
            resultsFileAnalysis['totalTime'],
            resultsFileAnalysis['throughput'],
            resultsFileAnalysis['avg'],
            resultsFileAnalysis['stddev'],
            resultsFileAnalysis['median'],
            resultsFileAnalysis['p90'],
            resultsFileAnalysis['p99'],
            resultsFileAnalysis['max'])

def compareRawResultsFiles(resultsFile1, resultsFile2):
    resultsFile1Analysis = analyzeRawResultsFile(resultsFile1)
    resultsFile2Analysis = analyzeRawResultsFile(resultsFile2)

    printRawResultsFileAnalysis(resultsFile1, resultsFile1Analysis)
    printRawResultsFileAnalysis(resultsFile2, resultsFile2Analysis)

    # Compare the results

    resultsComparisonsDiff = dict()
    resultsComparisonsPct = dict()
    for key in resultsFile1Analysis.keys():
        resultsComparisonsDiff[key] = float(resultsFile2Analysis[key] - resultsFile1Analysis[key])
        resultsComparisonsPct[key] = float(resultsComparisonsDiff[key] / resultsFile1Analysis[key] * 100)

    print 'ResultsFile1 vs ResultsFile2 {0:38s}: Transactions: {1:+12d}, Time (s): {2:+12.3f}, Throughput (txns/sec): {3:+12.3f},  Latencies (us):  Average: {4:+12.3f}, StdDev: {5:+12.3f}, Median: {6:+12.3f}, P90: {7:+12.3f}, P99: {8:+12.3f}, Max: {9:+12.3f}'.format(
            '', # padding
            int(resultsComparisonsDiff['totalTransactions']),
            resultsComparisonsDiff['totalTime'],
            resultsComparisonsDiff['throughput'],
            resultsComparisonsDiff['avg'],
            resultsComparisonsDiff['stddev'],
            resultsComparisonsDiff['median'],
            resultsComparisonsDiff['p90'],
            resultsComparisonsDiff['p99'],
            resultsComparisonsDiff['max'])

    print 'ResultsFile1 vs ResultsFile2 (%) {0:34s}: Transactions: {1:+11.3f}%, Time (s): {2:+11.3f}%, Throughput (txns/sec): {3:+11.3f}%,  Latencies (us):  Average: {4:+11.3f}%, StdDev: {5:+11.3f}%, Median: {6:+11.3f}%, P90: {7:+11.3f}%, P99: {8:+11.3f}%, Max: {9:+11.3f}%'.format(
            '', # padding
            resultsComparisonsPct['totalTransactions'],
            resultsComparisonsPct['totalTime'],
            resultsComparisonsPct['throughput'],
            resultsComparisonsPct['avg'],
            resultsComparisonsPct['stddev'],
            resultsComparisonsPct['median'],
            resultsComparisonsPct['p90'],
            resultsComparisonsPct['p99'],
            resultsComparisonsPct['max'])

def main():
    mode, resultsFile1, resultsFile2 = processArgs()
    if mode == 'aggregate':
        compareAggregateResultsFiles(resultsFile1, resultsFile2)
    elif mode == 'raw':
        compareRawResultsFiles(resultsFile1, resultsFile2)
    else:
        raise Exception('Unhandled mode: "{0}"'.format(mode))

if __name__ == "__main__":
    main()
