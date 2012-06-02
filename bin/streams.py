#!/usr/bin/env python3

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    LOGGER = logging.getLogger(__name__)

    import argparse

    aparser = argparse.ArgumentParser(description='Detect sequential streams.')
    aparser.add_argument('--trace', required=True, dest='trace', help='path to json trace file')
    aparser.add_argument('--csv', required=False, dest='csv', help='path to csv output file')
    aparser.add_argument('--plot', required=False, dest='plot', help='path to plot output file')
    aparser.add_argument('--after', required=False, dest='after', type=int, help='consider commands after')
    aparser.add_argument('--before', required=False, dest='before', type=int, help='consider commands before')
    aparser.add_argument('--lbaLess', required=False, dest='lbaLess', type=int, help='consider commands with LBA less than')
    aparser.add_argument('--lbaGreater', required=False, dest='lbaGreater', type=int, help='consider commands with LBA greater than')

    args = aparser.parse_args()

    from pytrace.json_reader import JsonReader
    from pytrace.sata import Parser
    from pytrace.command_statistics import commandsToStats, commandsToStatCSV, Time, LBA, Length, CommandType
    from sequential_stream.detector import Detector
    from sequential_stream.field import Stream
    from itertools import tee, zip_longest, dropwhile, takewhile

    commands = Parser(JsonReader(args.trace))
    commands = Detector(commands, attrName='stream1', tDelta=1)
    commands = Detector(commands, attrName='stream10', tDelta=10)
    commands = Detector(commands, attrName='stream100', tDelta=100)
    commands = Detector(commands, attrName='stream1000', tDelta=1000)
    commands = Detector(commands, attrName='stream10000', tDelta=10000)
    commands = Detector(commands, attrName='stream100000', tDelta=100000)
    if args.after is not None:
        commands = dropwhile(lambda c: c.sTime() / 1000000 < args.after, commands)
    if args.before is not None:
        commands = takewhile(lambda c: c.sTime() / 1000000 <= args.before, commands)
    if args.lbaLess is not None:
        commands = filter(lambda c: c.start().lba < args.lbaLess, commands)
    if args.lbaGreater is not None:
        commands = filter(lambda c: c.start().lba <= args.lbaGreater, commands)


    outputs = []

    statFields = [Time(True), LBA(), Length(), CommandType(), Stream('stream10'), Stream('stream100'), Stream('stream1000'), Stream('stream10000'), Stream('stream100000')]

    if args.csv is not None:
        outputs.append(lambda cmds: commandsToStatCSV(args.csv, cmds, fields=statFields))

    import matplotlib
    matplotlib.use('svg')
    import matplotlib.pyplot as plt
    if args.plot is not None:
        from os.path import splitext
        name, ext = splitext(args.plot)
        def plotOutputter(cmds):
            from matplotlib.ticker import FormatStrFormatter
            from functools import reduce
            from collections import defaultdict
            from numpy import fromiter,array,empty_like
            import gc

            def plot(plotFn, customFmt=lambda ax: None, subplot=(1,1,1), title=None, xlabel=None, ylabel=None):
                LOGGER.info('Plotting %s' % title)
                ax = plt.subplot(subplot[0], subplot[1], subplot[2])
                ax.legend_ = None
                if title is not None:
                    ax.set_title(title, fontsize=20)
                if xlabel is not None:
                    ax.set_xlabel(xlabel, fontsize=16)
                if ylabel is not None:
                    ax.set_ylabel(ylabel, fontsize=16)
                plotFn()
                plt.setp(ax.get_xticklabels(), fontsize=10)
                plt.setp(ax.get_yticklabels(), fontsize=10)
                customFmt(ax)

            streamKeys = {
                'stream10' : 10,
                'stream100' : 100,
                'stream1000' : 1000,
                'stream10000' : 10000,
                'stream100000' : 100000
            }

            def accumulator(accum, s):
                t = s['Start Time']
                lba = s['LBA']
                if s['Cmd'] is 'R':
                    accum['rLBA'].append(lba)
                    accum['rTime'].append(t)
                elif s['Cmd'] is 'W':
                    accum['wLBA'].append(lba)
                    accum['wTime'].append(t)
                for k, _ in streamKeys.items():
                    accum[k][s[k]].append(s['Start Time'])
                return accum

            accum = { k : defaultdict(list) for k, _ in streamKeys.items() }
            accum['rLBA'] = list()
            accum['rTime'] = list()
            accum['wLBA'] = list()
            accum['wTime'] = list()
            accum['ID'] = list()
            LOGGER.info('Accumulating')
            streams = reduce(accumulator, commandsToStats(cmds, fields=statFields), accum)
            accum['rLBA'] = array(accum['rLBA'])
            accum['rTime'] = array(accum['rTime'])
            accum['wLBA'] = array(accum['wLBA'])
            accum['wTime'] = array(accum['wTime'])
            accum['ID'] = array(accum['ID'])
            for k in sorted(streamKeys.keys()):
                streams[k] = {i : array(t) for i, t in streams[k].items() }
            gc.collect()

            def save(t):
                plt.savefig('%s-%s%s' % (name, t, ext))
                plt.cla()
                plt.clf()
         
            rows = 1
            cols = 1
            n = 1
            def rwPlot():
                plt.plot(streams['rTime'], streams['rLBA'], 'b.', markersize=3.0)
                plt.plot(streams['wTime'], streams['wLBA'], 'r.', markersize=3.0)
            plot(rwPlot, lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            save('lba')
            plot(lambda: plt.plot(streams['rTime'], streams['rLBA'], 'b.', markersize=3.0),
                    lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='Read LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            save('read_lba')
            plot(lambda: plt.plot(streams['wTime'], streams['wLBA'], 'r.', markersize=3.0),
                    lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='Write LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            save('write_lba')
            del streams['rLBA']
            del streams['rTime']
            del streams['wLBA']
            del streams['wTime']
            for k in sorted(streamKeys.keys()):
                gc.collect()
                v = streamKeys[k]
                def plotStreams():
                    for i, t in streams[k].items():
                        t2 = empty_like(t)
                        t2.fill(i)
                        plt.plot(t, t2, '.', markersize=3.0)
                plot(plotStreams, subplot=(rows, cols, n), title=str(v) + 'ms Streams', xlabel='Time (sec)', ylabel='Stream ID')
                save('%s_streams' % v)
                gc.collect()
                ids = fromiter([ i for i, _ in streams[k].items() if i is not -1 ], int)
                plot(lambda: plt.bar(ids, fromiter([ len(t) for i, t in streams[k].items() if i is not -1 ], int)),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (commands)', xlabel='Stream ID',
                        ylabel='Number of Commands')
                save('%s_count' % v)
                gc.collect()
                plot(lambda: plt.bar(ids, fromiter([ t[len(t) - 1] - t[0] for i, t in streams[k].items() if i is not -1 ], int)),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (duration)', xlabel='Stream ID',
                        ylabel='Duration (sec)')
                save('%s_duration' % v)
                del streams[k]
                gc.collect()

        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])
