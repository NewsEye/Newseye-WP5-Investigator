# -*- coding: utf-8 -*-
"""
This class represents a progress bar. It displays progress in a way that does not clutter up the output.
If possible, it also estimates the remaining time.
ETR: Estimated time remaining
ET: Elapsed time
@Author Llorenç
"""
import sys, time, datetime


##Used to print nice progress bars. Potentially estimating remaining time.
class ProgressBar(object):

    ##Construct a progress bar object
    def __init__(
        self, message, verbose=True, total=None, barLength=30, printPeriod=0.5, out=sys.stderr,
    ):
        self.progressCount = 0
        self.progressStart = 0
        self.prevTime = 0
        self.prevLen = 0
        self.message = message
        self.total = total
        self.barLength = barLength
        self.printPeriod = printPeriod
        self.out = out
        self.verbose = verbose

    # Update fields before printing, including the message
    def updateFields(self, message):
        if message != None:
            self.message = message
        self.prevTime = time.time()

    ##Generate the actual output line
    def makeLine(self, message, final=False):
        self.updateFields(message)
        speed = self.progressCount / (self.prevTime - self.progressStart)
        ret = self.message
        if self.total is None:
            ret += " ({0} {1:.2f}/s".format(
                self.progressCount, self.progressCount / (self.prevTime - self.progressStart),
            )
        else:
            try:
                remainingTime = datetime.timedelta(
                    seconds=int((self.total - self.progressCount) / speed)
                )
            except ZeroDivisionError:
                remainingTime = 0
            try:
                barDone = int(round((float(self.progressCount) / self.total) * self.barLength))
            except ZeroDivisionError:
                barDone = 0
            try:
                percentDone = int(round(100 * float(self.progressCount) / self.total))
            except ZeroDivisionError:
                percentDone = 0
            ret += " [{0}] {1}% ({2}/{3} {4:.2f}/s".format(
                "#" * barDone + " " * (self.barLength - barDone),
                percentDone,
                self.progressCount,
                self.total,
                speed,
            )
            if not final:  # If we progress is not done, add remaining estimate
                ret += " ETR {0}".format(remainingTime)
        if final:  # If progress is done, add how long it took
            ret += " ET {0}".format(
                datetime.timedelta(seconds=int(time.time() - self.progressStart))
            )
        ret += ")"
        return ret

    ##Refresh the output
    def refreshLine(self, message):
        if not self.verbose:
            return
        self.out.write("\r" + message)
        if self.prevLen > len(message):
            self.out.write(" " * (self.prevLen - len(message)))
        self.prevLen = len(message)
        self.out.flush()

    ##Should be called at each iteration, it will prevent excessive printing
    def next(self, message=None, count=1):
        if self.progressStart == 0:
            self.progressStart = time.time() - 1
        self.progressCount += count
        if time.time() < self.prevTime + self.printPeriod:
            return
        self.refreshLine(self.makeLine(message))

    ##Used to ensure that the final print is done (essentially to show completion) and feed a new line
    def end(self, message=None):
        self.updateFields(message)
        self.refreshLine(self.makeLine(message, final=True) + "\n")
